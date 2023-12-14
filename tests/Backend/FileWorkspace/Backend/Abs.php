<?php

namespace Keboola\Test\Backend\FileWorkspace\Backend;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Downloader\AbsUrlParser;
use Keboola\StorageApi\ProcessPolyfill;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Exception\ProcessFailedException;

class Abs
{
    /** @var string */
    private $connectionString;

    /** @var mixed */
    private $container;

    public function __construct(array $connection)
    {
        $this->connectionString = $connection['connectionString'];
        $this->container = $connection['container'];
    }

    /**
     * @param string|null $prefix
     * @return Blob[]
     */
    public function listFiles($prefix)
    {
        $options = new ListBlobsOptions();
        if ($prefix !== null) {
            $options->setPrefix($prefix);
        }
        $blobs = $this->getClient()->listBlobs($this->container, $options);
        return $blobs->getBlobs();
    }

    /**
     * @return BlobRestProxy
     */
    public function getClient()
    {
        $blobClient = BlobRestProxy::createBlobService($this->connectionString);
        $blobClient->pushMiddleware(RetryMiddlewareFactory::create());
        return $blobClient;
    }

    /**
     * @param string $prefix
     * @param string[] $columns
     * @param bool $isCompressed
     * @return int
     */
    public function countRows($prefix, $columns, $isCompressed)
    {
        $data = $this->fetchAll($prefix, $columns, $isCompressed, true);
        return count($data);
    }

    /**
     * @param string $prefix
     * @param string[] $columns
     * @param bool $isCompressed
     * @param bool $skipHeader
     * @param bool $isSliced
     * @return array
     */
    public function fetchAll(
        $prefix,
        $columns,
        $isCompressed,
        $skipHeader,
        $isSliced = true
    ) {
        $destination = $this->exportFile(
            $prefix,
            $isCompressed,
            $columns,
            $isSliced,
        );
        $file = new CsvFile($destination);

        $data = [];
        foreach ($file as $i => $row) {
            if ($skipHeader === true && $i === 0) {
                // skip header
                continue;
            }
            $data[] = $row;
        }

        return $data;
    }

    /**
     * @return string
     */
    public function uploadTestingFile()
    {
        $filePath = (string) tempnam(sys_get_temp_dir(), 'abs-file-upload');
        file_put_contents($filePath, 'We fight for data');
        $this->uploadFile($filePath, basename($filePath));

        return basename($filePath);
    }

    /**
     * @param string $filePath
     * @param string $destinationName
     */
    public function uploadFile($filePath, $destinationName)
    {
        $content = fopen($filePath, 'rb');
        $this->getClient()->createBlockBlob($this->container, $destinationName, $content);
    }

    /**
     * @param string $prefix
     * @param bool $isCompressed
     * @param string[] $columns
     * @param bool $isSliced
     * @return string
     */
    private function exportFile($prefix, $isCompressed, $columns, $isSliced)
    {
        $client = $this->getClient();
        $workingDir = sys_get_temp_dir();
        $tmpFilePath = $workingDir . '/' . uniqid('sapi-abs-workspace-', true);
        $destination = $workingDir . '/' . uniqid('sapi-abs-workspace-dest-', true) . '.csv';
        $fs = new Filesystem();
        if (!$fs->exists($workingDir)) {
            $fs->mkdir($workingDir);
        }

        if ($fs->exists($destination)) {
            $fs->remove($destination);
        }

        $files = [];
        if ($isSliced) {
            $blob = $client->getBlob(
                $this->container,
                $prefix . 'manifest',
            );
            $blob = json_decode(stream_get_contents($blob->getContentStream()), true);
            foreach ($blob['entries'] as $entry) {
                list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($entry['url']);

                $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $file));

                $result = $client->getBlob(
                    $container,
                    $file,
                );
                file_put_contents($filePath, $result->getContentStream());
                $files[] = $filePath;
            }
        } else {
            $options = new ListBlobsOptions();
            $options->setPrefix($prefix);
            $blobs = $client->listBlobs($this->container, $options);
            foreach ($blobs->getBlobs() as $blob) {
                $filePath = $tmpFilePath . '_' . md5(str_replace('/', '_', $blob->getName()));
                $blobResult = $client->getBlob(
                    $this->container,
                    $blob->getName(),
                );
                file_put_contents($filePath, $blobResult->getContentStream());
                $files[] = $filePath;
            }
        }

        // Create file with header
        $delimiter = ',';
        $enclosure = '"';
        $header = $enclosure . implode($enclosure . $delimiter . $enclosure, $columns) . $enclosure . "\n";
        if ($isCompressed === true) {
            $fs->dumpFile($destination . '.tmp', $header);
        } else {
            $fs->dumpFile($destination, $header);
        }

        // Concat all files into one, compressed files need to be decompressed first
        foreach ($files as $file) {
            if ($isCompressed) {
                $catCmd = 'gunzip ' . escapeshellarg($file) . ' --to-stdout >> ' . escapeshellarg($destination) . '.tmp';
            } else {
                $catCmd = 'cat ' . escapeshellarg($file) . ' >> ' . escapeshellarg($destination);
            }
            $process = ProcessPolyfill::createProcess($catCmd);
            $process->setTimeout(null);
            if (0 !== $process->run()) {
                throw new ProcessFailedException($process);
            }
            $fs->remove($file);
        }

        if ($isCompressed) {
            $fs->rename($destination . '.tmp', $destination);
        }
        return $destination;
    }
}
