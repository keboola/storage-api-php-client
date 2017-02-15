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

use Aws\S3\S3Client;
use GuzzleHttp\Client as HttpClient;
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

    /**
     *
     * Process async export and prepare the file on disk
     *
     * @param $tableId string SAPI Table Id
     * @param $destination string destination file
     * @param $exportOptions array SAPI Client export options
     * @return void
     */
    public function exportTable($tableId, $destination, $exportOptions)
    {
        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        // Export table from Storage API to S3
        $table = $this->client->getTable($tableId);
        $fileId = $this->client->exportTableAsync($tableId, $exportOptions);
        $fileInfo = $this->client->getFile($fileId["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        // Initialize S3Client with credentials from Storage API
        $s3Client = new S3Client([
            'version' => '2006-03-01',
            'region' => $fileInfo['region'],
            'retries' => $this->client->getAwsRetries(),
            'credentials' => [
                'key' => $fileInfo["credentials"]["AccessKeyId"],
                'secret' => $fileInfo["credentials"]["SecretAccessKey"],
                'token' => $fileInfo["credentials"]["SessionToken"],
            ],
        ]);

        // Temporary folder to save downloaded files from S3
        $workingDir = sys_get_temp_dir() . '/sapi-php-client';


        $fs = new Filesystem();
        if (!$fs->exists($workingDir)) {
            $fs->mkdir($workingDir);
        }

        if ($fs->exists($destination)) {
            $fs->remove($destination);
        }

        $tmpFilePath = $workingDir . '/' . uniqid('sapi-export-');

        if ($fileInfo['isSliced'] === true) {
            /**
             * sliced file - combine files together
             */

            // Download manifest with all sliced files
            $client = new HttpClient([
                'handler' => HandlerStack::create([
                    'backoffMaxTries' => 10,
                ]),
            ]);
            $manifest = json_decode($client->get($fileInfo['url'])->getBody(), true);
            $files = array();

            // Download all sliced files
            foreach ($manifest["entries"] as $part) {
                $fileKey = substr($part["url"], strpos($part["url"], '/', 5) + 1);
                $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $fileKey));
                $files[] = $filePath;
                $s3Client->getObject(array(
                    'Bucket' => $fileInfo["s3Path"]["bucket"],
                    'Key' => $fileKey,
                    'SaveAs' => $filePath
                ));
            }

            // Create file with header
            $format = 'rfc';
            if (isset($exportOptions['format'])) {
                $format = $exportOptions['format'];
            }
            switch ($format) {
                case 'raw':
                    $delimiter = "\t";
                    $enclosure = "";
                    break;
                case 'rfc':
                    $delimiter = ",";
                    $enclosure = '"';
                    break;
                case 'escaped':
                    $delimiter = ",";
                    $enclosure = '"';
                    break;
            }

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
                $process = new Process($catCmd);
                $process->setTimeout(null);
                if (0 !== $process->run()) {
                    throw new ProcessFailedException($process);
                }
                $fs->remove($file);
            }

            // Compress the file afterwards if required
            if ($exportOptions["gzip"]) {
                $gZipCmd = "gzip " . escapeshellarg($destination) . ".tmp --fast";
                $process = new Process($gZipCmd);
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
            $s3Client->getObject(array(
                'Bucket' => $fileInfo["s3Path"]["bucket"],
                'Key' => $fileInfo["s3Path"]["key"],
                'SaveAs' => $tmpFilePath
            ));
            $fs->rename($tmpFilePath, $destination);
        }

        $fs->remove($tmpFilePath);

        return;
    }
}
