<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Export;

use Google\Cloud\Core\Exception\ServiceException;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;

class ExportParamsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testTableExportAsyncSliced($exportOptions, $expectedResult): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $csvFile = new CsvFile($importFile);
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', $csvFile, [
            'columns' => $csvFile->getHeader(),
        ]);

        $results = $this->_client->exportTableAsync($tableId, array_merge($exportOptions, [
            'format' => 'rfc',
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

        $csv = '';
        foreach ($slices as $slice) {
            $csv .= file_get_contents($slice);
        }

        $parsedData = Client::parseCsv($csv, false, ',', '"');
        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

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
            foreach ($objects['Contents'] as $object) {
                $objectDetail = $s3Client->headObject([
                    'Bucket' => $bucket,
                    'Key' => $object['Key'],
                ]);

                $this->assertEquals('AES256', $objectDetail['ServerSideEncryption']);
                $this->assertStringStartsWith($prefix, $object['Key']);
            }
        }
    }
}
