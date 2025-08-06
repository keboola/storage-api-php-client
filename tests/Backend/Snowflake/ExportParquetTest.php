<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Exporter\FileType;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\Backend\Bigquery\TestExportDataProvidersTrait;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\ParquetUtils;
use Symfony\Component\Filesystem\Filesystem;

class ExportParquetTest extends StorageApiTestCase
{
    use TestExportDataProvidersTrait;
    use ParquetUtils;

    private string $downloadPath;

    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
        $this->downloadPath = $this->getExportFilePathForTest('languages.sliced.parquet');
    }

    /**
     * @dataProvider tableExportData
     */
    public function testTableAsyncExport(CsvFile $importFile, string $expectationsFileName, array $exportOptions = [], string|int $orderBy = 'id'): void
    {
        $expectationsFile = __DIR__ . '/../../_data/bigquery/' . $expectationsFileName;

        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $exportOptions['fileType'] = FileType::PARQUET;

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', $importFile);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $exporter = new TableExporter($this->_client);

        if (isset($exportOptions['columns'])) {
            $expectedColumns = $exportOptions['columns'];
        } else {
            $table = $this->_client->getTable($tableId);
            $expectedColumns = $table['columns'];
        }

        $exporter->exportTable($tableId, $this->downloadPath, $exportOptions);

        // compare data
        $files = glob($this->downloadPath . '/*');
        $this->assertNotEmpty($files, 'No files found in download path: ' . $this->downloadPath);

        $content = $this->getParquetContent($files);

        $this->assertArrayEqualsSorted(
            Client::parseCsv((string) file_get_contents($expectationsFile), true),
            $content,
            $orderBy,
            'imported data comparison',
        );

        // check that columns has been set in export job params
        $jobs = $this->listJobsByRunId($runId);
        $job = reset($jobs);

        $this->assertSame($runId, $job['runId']);
        $this->assertSame('tableExport', $job['operationName']);
        $this->assertSame($tableId, $job['tableId']);
        $this->assertNotEmpty($job['operationParams']['export']['columns']);
        $this->assertSame($expectedColumns, $job['operationParams']['export']['columns']);
        $this->assertTrue($job['operationParams']['export']['gzipOutput']);
        $this->assertSame(2, $job['operationParams']['export']['fileType']);
    }

    // bigquery exports data different, so we create new files for BQ
    public function tableExportData(): array
    {
        $filesBasePath = __DIR__ . '/../../_data/bigquery/';
        return [
            [new CsvFile($filesBasePath . '1200.csv'), '1200.csv', [], 'col_1'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv'],
            [new CsvFile($filesBasePath . 'languages.encoding.csv'), 'languages.encoding.csv'],
            [new CsvFile($filesBasePath . 'languages.csv.gz'), 'languages.csv', ['gzip' => true]],

            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv'],
            [new CsvFile($filesBasePath . 'languages.64k.csv'), 'languages.64k.csv',  ['gzip' => true]],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv'],
            [new CsvFile($filesBasePath . '64K.csv'), '64K.csv',  ['gzip' => true]],

            [new CsvFile($filesBasePath . 'escaping.csv'), 'escaping.standard.out.csv', ['gzip' => true], 'col1'],
        ];
    }

    /**
     * Tests downloadSlicedFile method with sliced Parquet files
     *
     * @param array<mixed> $exportOptions
     * @param array<mixed> $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testTableExportAsyncSlicedParquet($exportOptions, $expectedResult): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, [
            'columns' => $csvFile->getHeader(),
        ]);

        $results = $this->_client->exportTableAsync($tableId, array_merge($exportOptions, [
            'format' => 'rfc',
            'fileType' => FileType::PARQUET,
        ]));

        $exportedFile = $this->_client->getFile($results['file']['id'], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertTrue($exportedFile['isSliced']);
        $this->assertGreaterThan(0, $exportedFile['sizeBytes']);

        $tmpDestinationFolder = __DIR__ . '/../_tmp/slicedUpload/';
        $fs = new Filesystem();
        if (file_exists($tmpDestinationFolder)) {
            $fs->remove($tmpDestinationFolder);
        }
        $fs->mkdir($tmpDestinationFolder);

        $slices = $this->_client->downloadSlicedFile($results['file']['id'], $tmpDestinationFolder);
        $content = $this->getParquetContentNoKeys($slices);

        $this->assertArrayEqualsSorted(
            $expectedResult,
            $content,
            0,
            'imported data comparison',
        );

        if ($exportedFile['provider'] === Client::FILE_PROVIDER_GCP) {
            $params = $exportedFile['gcsCredentials'];

            $client = $this->getGcsClientClient($params);
            $bucket = $client->bucket($exportedFile['gcsPath']['bucket']);

            try {
                $objects = $bucket->objects();
                foreach ($objects as $object) {
                    $object->info();
                }
                $this->fail('List on all bucket should fail');
            } catch (ServiceException $e) {
                $this->assertSame(403, $e->getCode());
            }

            $prefix = $exportedFile['gcsPath']['key'];
            $objects = $bucket->objects(['prefix' => $prefix]);
            foreach ($objects as $object) {
                $this->assertStringStartsWith($exportedFile['gcsPath']['key'], $object->info()['name']);
            }
        } elseif ($exportedFile['provider'] === Client::FILE_PROVIDER_AZURE) {
            // Check ABC ACL and listing blobs
            $blobClient = BlobClientFactory::createClientFromConnectionString(
                $exportedFile['absCredentials']['SASConnectionString'],
            );
            $listResult = $blobClient->listBlobs($exportedFile['absPath']['container']);
            $table = $this->_client->getTable($tableId);
            if ($table['bucket']['backend'] !== self::BACKEND_SYNAPSE) {
                // polybase export in synapse is exporting more blobs
                // also manifest and folder it's just weird
                $this->assertCount(2, $listResult->getBlobs());
            }

            foreach ($listResult->getBlobs() as $blob) {
                $blobClient->getBlob(
                    $exportedFile['absPath']['container'],
                    $blob->getName(),
                );
            }
        } else {
            // Check S3 ACL and listing bucket
            $s3Client = new \Aws\S3\S3Client([
                'credentials' => [
                    'key' => $exportedFile['credentials']['AccessKeyId'],
                    'secret' => $exportedFile['credentials']['SecretAccessKey'],
                    'token' => $exportedFile['credentials']['SessionToken'],
                ],
                'version' => 'latest',
                'region' => $exportedFile['region'],
            ]);
            $bucket = $exportedFile['s3Path']['bucket'];
            $prefix = $exportedFile['s3Path']['key'];
            $objects = $s3Client->listObjects([
                'Bucket' => $bucket,
                'Prefix' => $prefix,
            ]);
            //@phpstan-ignore-next-line
            foreach ($objects['Contents'] as $object) {
                $objectDetail = $s3Client->headObject([
                    'Bucket' => $bucket,
                    //@phpstan-ignore-next-line
                    'Key' => $object['Key'],
                ]);

                $this->assertEquals('AES256', $objectDetail['ServerSideEncryption']);
                //@phpstan-ignore-next-line
                $this->assertStringStartsWith($prefix, $object['Key']);
            }
        }
    }
}
