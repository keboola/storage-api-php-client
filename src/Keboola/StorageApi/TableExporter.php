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
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

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

    private function handleExportedFile($tableId, $fileId, $destination, $exportOptions)
    {
        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $table = $this->client->getTable($tableId);
        $getFileResponse = $this->client->getFile(
            $fileId,
            (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true)
        );

        // Temporary folder to save downloaded files
        $workingDir = sys_get_temp_dir() . '/sapi-php-client';
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
            $this->client->getAwsRetries()
        );

        if ($getFileResponse['isSliced'] === true) {
            /**
             * sliced file - combine files together
             */

            // Download manifest with all sliced files
            $client = new HttpClient([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($getFileResponse['url'])->getBody(), true);
            $files = [];

            // Download all sliced files
            foreach ($manifest["entries"] as $part) {
                $files[] = $downloader->downloadManifestEntry($getFileResponse, $part, $tmpFilePath);
            }

            // Create file with header
            $delimiter = ",";
            $enclosure = '"';

            if (isset($exportOptions["columns"])) {
                $columns = $exportOptions["columns"];
            } else {
                $columns = $table["columns"];
            }

            $header = $enclosure . join($enclosure . $delimiter . $enclosure, $columns) . $enclosure . "\n";
            if ($exportOptions["gzip"] === true) {
                $fs->dumpFile($destination . '.tmp', $header);
            } else {
                $fs->dumpFile($destination, $header);
            }

            // Concat all files into one, compressed files need to be decompressed first
            foreach ($files as $file) {
                if ($exportOptions["gzip"]) {
                    $catCmd = "gunzip " . escapeshellarg($file) . " --to-stdout >> " . escapeshellarg($destination) . ".tmp";
                } else {
                    $catCmd = "cat " . escapeshellarg($file) . " >> " . escapeshellarg($destination);
                }
                $process = ProcessPolyfill::createProcess($catCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->remove($file);
            }

            // Compress the file afterwards if required
            if ($exportOptions["gzip"]) {
                $gZipCmd = "gzip " . escapeshellarg($destination) . ".tmp --fast";
                $process = ProcessPolyfill::createProcess($gZipCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->rename($destination . '.tmp.gz', $destination);
            }
        } else {
            /**
             * NonSliced file, just move from temp to destination file
             */
            $downloader->downloadFileFromFileResponse($getFileResponse, $tmpFilePath);
            $fs->rename($tmpFilePath, $destination);
        }
        $fs->remove($tmpFilePath);
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
                'exportOptions' => $exportOptions
            ]
        ]);
    }

    /**
     * @param array $tables
     * @throws Exception
     */
    public function exportTables(array $tables = array())
    {
        $exportJobs = [];
        foreach ($tables as $table) {
            if (empty($table['tableId'])) {
                throw new Exception('Missing tableId');
            }
            if (empty($table['destination'])) {
                throw new Exception('Missing destination');
            }
            if (!isset($table['exportOptions'])) {
                $table['exportOptions'] = array();
            }
            if (empty($table['exportOptions']['columns'])) {
                $tableDetail = $this->client->getTable($table['tableId']);
                $table['exportOptions']['columns'] = $tableDetail['columns'];
            }

            $jobId = $this->client->queueTableExport($table['tableId'], $table['exportOptions']);
            $exportJobs[$jobId] = $table;
        }
        $jobResults = $this->client->handleAsyncTasks(array_keys($exportJobs));
        foreach ($jobResults as $jobResult) {
            $exportJob = $exportJobs[$jobResult['id']];
            $this->handleExportedFile($exportJob['tableId'], $jobResult['results']['file']['id'], $exportJob['destination'], $exportJob['exportOptions']);
        }
    }
}
