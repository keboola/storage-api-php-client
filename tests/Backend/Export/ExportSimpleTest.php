<?php
namespace Keboola\Test\Backend\Export;

use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;

class ExportSimpleTest extends StorageApiTestCase
{
    /**
     * @return void
     */
    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * Copy of ExportParamsTest::testTableExportAsyncSliced with simple filters.
     * @see \Keboola\Test\Backend\Export\ExportParamsTest::testTableExportAsyncSliced
     *
     * @dataProvider tableExportSimpleProvider
     *
     * @param int $expectedColumnsCount
     * @param array[] $ignoreColumnKeys
     * @return void
     */
    public function testTableExportAsyncSliced(array $exportOptions, array $expectedResult, $expectedColumnsCount, array $ignoreColumnKeys)
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, array(
            'columns' => $csvFile->getHeader(),
        ));

        $results = $this->_client->exportTableAsync($tableId, array_merge($exportOptions, array(
            'format' => 'rfc',
        )));

        $exportedFile = $this->_client->getFile($results['file']['id'], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

        $this->assertTrue($exportedFile['isSliced']);
        $this->assertGreaterThan(0, $exportedFile['sizeBytes']);


        $tmpDestinationFolder = sprintf(
            '%s/%s',
            sys_get_temp_dir(),
            uniqid('slicedUpload', true)
        );
        $fs = new Filesystem();
        $fs->mkdir($tmpDestinationFolder);

        $slices = $this->_client->downloadSlicedFile($results['file']['id'], $tmpDestinationFolder);

        $csv = '';
        foreach ($slices as $slice) {
            $csv .= file_get_contents($slice);
        }

        $parsedData = Client::parseCsv($csv, false, ",", '"');
        $this->assertCount($expectedColumnsCount, $parsedData);
        // unset column (e.g. with datetime)
        foreach ($parsedData as &$parsedLine) {
            foreach ($ignoreColumnKeys as $ignoreColumnKey) {
                unset($parsedLine[$ignoreColumnKey]);
            }
        }
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

        if ($exportedFile['provider'] === Client::FILE_PROVIDER_AZURE) {
            // Check ABC ACL and listing blobs
            $blobClient = BlobClientFactory::createClientFromConnectionString(
                $exportedFile['absCredentials']['SASConnectionString']
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
                    $blob->getName()
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
                'region' => $exportedFile['region']
            ]);
            $bucket = $exportedFile["s3Path"]["bucket"];
            $prefix = $exportedFile["s3Path"]["key"];
            /** @var array{Contents: array} $objects */
            $objects = $s3Client->listObjects(array(
                "Bucket" => $bucket,
                "Prefix" => $prefix
            ));
            /** @var array{Key: string} $object */
            foreach ($objects["Contents"] as $object) {
                $objectDetail = $s3Client->headObject([
                    'Bucket' => $bucket,
                    'Key' => $object['Key'],
                ]);

                // only check the 'manifest' file
                if (substr($object['Key'], -8) === 'manifest') {
                    $this->assertEquals('AES256', $objectDetail['ServerSideEncryption']);
                }
                $this->assertStringStartsWith($prefix, $object["Key"]);
            }
        }
    }

    /**
     * @return \Generator
     */
    public function tableExportSimpleProvider()
    {
        yield 'basic' => [
            [],
            [
                [
                    '1',
                    'martin',
                    'PRG',
                    'male',
                ],
                [
                    '2',
                    'klara',
                    'PRG',
                    'female',
                ],
                [
                    '3',
                    'ondra',
                    'VAN',
                    'male',
                ],
                [
                    '4',
                    'miro',
                    'BRA',
                    'male',
                ],
                [
                    '5',
                    'hidden',
                    '',
                    'male',
                ],
            ],
            5, // count columns before ignore by keys
            [4], // ignore columns by keys
        ];
    }
}
