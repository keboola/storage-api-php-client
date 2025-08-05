<?php
/**
 * Storage API Client - Table Exporter
 *
 * Downloads a table from Storage API and saves it to destination path. Merges all parts of a sliced file and adds
 * header if missing.
 *
 * @author Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
 * @date: 5.6.14
 */

namespace Keboola\StorageApi;

use GuzzleHttp\Client as HttpClient;
use Keboola\StorageApi\Downloader\DownloaderFactory;
use Keboola\StorageApi\Downloader\DownloaderInterface;
use Keboola\StorageApi\Exporter\DownloadedSliceEntry;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Exception\ProcessFailedException;

class TableExporter
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    private function handleExportedFile($tableId, $fileId, $destination, $exportOptions, $gzipOutput)
    {
        try {
            $table = $this->client->getTable($tableId);
        } catch (ClientException $e) {
            if ($this->client instanceof BranchAwareClient && $e->getCode() === 404) {
                // try to get table from default branch if not found in dev branch
                $table = $this->client->getDefaultBranchClient()->getTable($tableId);
            } else {
                throw $e;
            }
        }
        $getFileResponse = $this->client->getFile(
            $fileId,
            (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true),
        );

        // Temporary folder to save downloaded files
        $workingDir = sys_get_temp_dir();
        $tmpFilePath = $workingDir . '/' . uniqid('sapi-export-', true);

        $fs = new Filesystem();
        if (!$fs->exists($workingDir)) {
            $fs->mkdir($workingDir);
        }

        if ($fs->exists($destination)) {
            $fs->remove($destination);
        }

        $downloader = DownloaderFactory::createDownloaderForFileResponse(
            $getFileResponse,
            $this->client->getAwsRetries(),
        );

        if ($getFileResponse['isSliced'] === true && $getFileResponse['fileType'] === 'csv') {
            /**
             * sliced file - combine files together
             * Only CSV files are combined
             */
            $fileSlices = $this->downloadSlices(
                $getFileResponse,
                $downloader,
                $tmpFilePath,
            );

            // Create file with header
            $delimiter = ',';
            $enclosure = '"';

            if (isset($exportOptions['columns'])) {
                $columns = $exportOptions['columns'];
            } else {
                $columns = $table['columns'];
            }

            if (array_key_exists('includeInternalTimestamp', $exportOptions) && $exportOptions['includeInternalTimestamp']) {
                $columns[] = '_timestamp';
            }

            $header = $enclosure . join($enclosure . $delimiter . $enclosure, $columns) . $enclosure . "\n";
            $fs->dumpFile($destination . '.tmp', $header);

            // Concat all files into one, compressed files need to be decompressed first
            foreach ($fileSlices as $fileSlice) {
                $catCmd = 'gunzip ' . escapeshellarg($fileSlice->filePath) . ' --to-stdout >> ' . escapeshellarg($destination . '.tmp');
                $process = ProcessPolyfill::createProcess($catCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->remove($fileSlice->filePath);
            }

            // Compress the file afterwards if required
            if ($gzipOutput) {
                $gZipCmd = 'gzip ' . escapeshellarg($destination . '.tmp') . ' --fast';
                $process = ProcessPolyfill::createProcess($gZipCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->rename($destination . '.tmp.gz', $destination);
            } else {
                $fs->rename($destination . '.tmp', $destination);
            }
        } elseif ($getFileResponse['isSliced'] === true) {
            /**
             * Sliced file, but not CSV - download all slices
             * This is used for files like Parquet
             */
            $fileSlices = $this->downloadSlices(
                $getFileResponse,
                $downloader,
                $tmpFilePath,
            );
            $fs->mkdir($destination);
            foreach ($fileSlices as $fileSlice) {
                $sliceName = $fileSlice->getFileName($downloader);
                $fs->rename($fileSlice->filePath, $destination . '/' . $sliceName);
            }
        } else {
            /**
             * NonSliced file, just move from temp to destination file
             */
            $downloader->downloadFileFromFileResponse($getFileResponse, $tmpFilePath);
            if ($gzipOutput) {
                $fs->rename($tmpFilePath, $destination);
            } else {
                $catCmd = 'gunzip ' . escapeshellarg($tmpFilePath) . ' --to-stdout >> ' . escapeshellarg($destination);
                $process = ProcessPolyfill::createProcess($catCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->remove($tmpFilePath);
            }
        }
    }

    /**
     *
     * Process async export and prepare the file on disk
     *
     * @param $tableId
     * @param $destination
     * @param $exportOptions
     * @throws Exception
     */
    public function exportTable($tableId, $destination, $exportOptions)
    {
        $this->exportTables([
            [
                'tableId' => $tableId,
                'destination' => $destination,
                'exportOptions' => $exportOptions,
            ],
        ]);
    }

    /**
     * @param array $tables
     * @return array Job results
     * @throws Exception
     */
    public function exportTables(array $tables = [])
    {
        $exportJobs = [];
        $exportOptions = [];
        foreach ($tables as $table) {
            if (empty($table['tableId'])) {
                throw new Exception('Missing tableId');
            }
            if (empty($table['destination'])) {
                throw new Exception('Missing destination');
            }
            if (!isset($table['exportOptions'])) {
                $table['exportOptions'] = [];
            }
            if (empty($table['exportOptions']['columns'])) {
                try {
                    $tableDetail = $this->client->getTable($table['tableId']);
                } catch (ClientException $e) {
                    if ($this->client instanceof BranchAwareClient && $e->getCode() === 404) {
                        // try to get table from default branch if not found in dev branch
                        $tableDetail = $this->client->getDefaultBranchClient()->getTable($table['tableId']);
                    } else {
                        throw $e;
                    }
                }
                $table['exportOptions']['columns'] = $tableDetail['columns'];
            }
            $exportOptions[$table['tableId']] = $table['exportOptions'];

            $table['exportOptions']['gzip'] = true;
            $jobId = $this->client->queueTableExport($table['tableId'], $table['exportOptions']);
            $exportJobs[$jobId] = $table;
        }
        $jobResults = $this->client->handleAsyncTasks(array_keys($exportJobs));
        foreach ($jobResults as $jobResult) {
            $exportJob = $exportJobs[$jobResult['id']];
            $isGzip = false;
            if ($exportOptions[$exportJob['tableId']]['gzip'] === true) {
                $isGzip = true;
                if ($exportOptions[$exportJob['tableId']]['fileType'] === 'parquet') {
                    // parquet files are not gzipped, but snappy compressed
                    $isGzip = false;
                }
            }
            $this->handleExportedFile(
                $exportJob['tableId'],
                $jobResult['results']['file']['id'],
                $exportJob['destination'],
                $exportJob['exportOptions'],
                $isGzip,
            );
        }
        return $jobResults;
    }

    /**
     * @return DownloadedSliceEntry[]
     */
    private function downloadSlices(
        array $getFileResponse,
        DownloaderInterface $downloader,
        string $tmpFilePath
    ): array {
        // Download manifest with all sliced files
        $client = new HttpClient([
            'handler' => HandlerStack::create([
                'backoffMaxTries' => 10,
            ]),
        ]);
        $manifest = json_decode($client->get($getFileResponse['url'])->getBody(), true);
        assert(is_array($manifest) && isset($manifest['entries']), 'Invalid manifest format');
        $fileSlices = [];

        // Download all sliced files
        foreach ($manifest['entries'] as $part) {
            $fileSlices[] = new DownloadedSliceEntry(
                (string) $part['url'],
                $tmpFilePath,
                $downloader->downloadManifestEntry($getFileResponse, $part, $tmpFilePath),
            );
        }
        return $fileSlices;
    }
}
