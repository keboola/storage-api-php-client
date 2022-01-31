<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class SlicedImportsTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testSlicedImportGzipped()
    {
        $slices = [
            __DIR__ . '/../../_data/sliced/neco_0000_part_00.gz',
            __DIR__ . '/../../_data/sliced/neco_0001_part_00.gz',
        ];

        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(true)
            ->setIsSliced(true);

        $fileId = $this->_client->uploadSlicedFile(
            $slices,
            $uploadOptions
        );

        $headerFile = new CsvFile(__DIR__ . '/../../_data/sliced/header.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'entries', $headerFile, [
            'primaryKey' => 'id',
        ]);
        $table = $this->_client->getTable($tableId);
        if ($table['bucket']['backend'] === self::BACKEND_SYNAPSE) {
            $this->markTestSkipped('Empty ECLOSURE is not possible with synapse.');
        }
        $this->_client->writeTableAsyncDirect($tableId, array(
            'dataFileId' => $fileId,
            'columns' => $headerFile->getHeader(),
            'delimiter' => '|',
            'enclosure' => '',
        ));
    }

    public function testSlicedImportSingleFile()
    {
        $slices = [
            __DIR__ . '/../../_data/languages.no-headers.csv',
        ];

        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('languages_')
            ->setIsSliced(true)
            ->setIsEncrypted(false);

        $fileId = $this->_client->uploadSlicedFile(
            $slices,
            $uploadOptions
        );

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'entries',
            new CsvFile(__DIR__ . '/../../_data/languages.not-normalized-column-names.csv')
        );
        $this->_client->deleteTableRows($tableId);
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => [
                'language-id',
                'language-name',
            ]
        ]);

        $this->assertLinesEqualsSorted(
            file_get_contents(__DIR__ . '/../../_data/languages.normalized-column-names.csv'),
            $this->_client->getTableDataPreview($tableId, [
                'format' => 'rfc',
            ]),
            'imported data comparsion'
        );

        // incremental
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $fileId,
            'incremental' => true,
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => [
                'language-id',
                'language-name',
            ]
        ]);

        $data = file_get_contents(__DIR__ . '/../../_data/languages.normalized-column-names.csv');
        $lines = explode("\n", $data);
        array_shift($lines);
        $data = $data . implode("\n", $lines);

        $this->assertLinesEqualsSorted(
            $data,
            $this->_client->getTableDataPreview($tableId, [
                'format' => 'rfc',
            ]),
            'imported data comparsion'
        );
    }

    public function testSlicedImportMissingManifest()
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(false)
            ->setIsSliced(true);
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'entries', new CsvFile(__DIR__ . '/../../_data/sliced/header.csv'));

        try {
            $this->_client->writeTableAsyncDirect($tableId, array(
                'dataFileId' => $slicedFile['id'],
                'withoutHeaders' => true,
                'delimiter' => '|',
                'enclosure' => '',
                'escapedBy' => '',
            ));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.manifestFileMissing', $e->getStringCode());
        }
    }

    public function testInvalidFilesInManifest()
    {
        $tokenData = $this->_client->verifyToken();
        if ($tokenData['owner']['defaultBackend'] == self::BACKEND_REDSHIFT) {
            $this->markTestSkipped('TODO: redshift bug to fix');
        }

        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(false)
            ->setIsSliced(true);
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

        if ($slicedFile['provider'] === Client::FILE_PROVIDER_AZURE) {
            $uploadParams = $slicedFile['absUploadParams'];

            $blobClient = BlobClientFactory::createClientFromConnectionString(
                $uploadParams['absCredentials']['SASConnectionString']
            );
            $blobClient->createBlockBlob(
                $uploadParams['container'],
                sprintf(
                    '%s%s',
                    $uploadParams['blobName'],
                    'part001.gz'
                ),
                fopen(__DIR__ . '/../../_data/sliced/neco_0000_part_00.gz', 'r')
            );

            $blobClient->createBlockBlob(
                $uploadParams['container'],
                $uploadParams['blobName'] . 'manifest',
                json_encode([
                    'entries' => [
                        [
                            'url' => sprintf(
                                'azure://%s.blob.core.windows.net/%s/%s%s',
                                $uploadParams['accountName'],
                                $uploadParams['container'],
                                $uploadParams['blobName'],
                                'part001.gz'
                            ),
                        ],
                        [
                            'url' => sprintf(
                                'azure://%s.blob.core.windows.net/%s/%s%s',
                                $uploadParams['accountName'],
                                $uploadParams['container'],
                                $uploadParams['blobName'],
                                'part001.gzsome'
                            ),
                        ],
                    ],
                ])
            );
        } else {
            $uploadParams = $slicedFile['uploadParams'];

            $s3Client = new \Aws\S3\S3Client([
                'credentials' => [
                    'key' => $uploadParams['credentials']['AccessKeyId'],
                    'secret' => $uploadParams['credentials']['SecretAccessKey'],
                    'token' => $uploadParams['credentials']['SessionToken'],
                ],
                'version' => 'latest',
                'region' => $slicedFile['region'],
            ]);

            $s3Client->putObject(array(
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'part001.gz',
                'Body' => fopen(__DIR__ . '/../../_data/sliced/neco_0000_part_00.gz', 'r+'),
            ))->get('ObjectURL');

            $s3Client->putObject(array(
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'manifest',
                'Body' => json_encode([
                    'entries' => [
                        [
                            'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.gz',
                            'mandatory' => true,
                        ],
                        [
                            'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.gzsome',
                            'mandatory' => true,
                        ]
                    ],
                ]),
            ))->get('ObjectURL');
        }

        $tableId = $this->_client->createTable($this->getTestBucketId(), 'entries', new CsvFile(__DIR__ . '/../../_data/sliced/header.csv'));

        try {
            $this->_client->writeTableAsyncDirect($tableId, array(
                'dataFileId' => $slicedFile['id'],
                'columns' => array('id', 'added_manually', 'start', 'end', 'task_id', 'project_id'),
                'delimiter' => '|'
            ));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.importFileMissing', $e->getStringCode());
        }
    }

    public function testUnauthorizedAccessInManifestFile()
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'entries', new CsvFile(__DIR__ . '/../../_data/sliced/header.csv'));

        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('entries_')
            ->setIsEncrypted(false)
            ->setIsSliced(true);

        // First upload
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

        if ($slicedFile['provider'] === Client::FILE_PROVIDER_AZURE) {
            $uploadParams = $slicedFile['absUploadParams'];

            $blobClient = BlobClientFactory::createClientFromConnectionString(
                $uploadParams['absCredentials']['SASConnectionString']
            );

            $blobClient->createBlockBlob(
                $uploadParams['container'],
                sprintf(
                    '%s%s',
                    $uploadParams['blobName'],
                    'part001.gz'
                ),
                fopen(__DIR__ . '/../../_data/escaping.csv', 'r')
            );

            $part1URL = sprintf(
                'azure://%s.blob.core.windows.net/%s/%s%s',
                $uploadParams['accountName'],
                $uploadParams['container'],
                $uploadParams['blobName'],
                'part001.gz'
            );
        } else {
            $uploadParams = $slicedFile['uploadParams'];
            $s3Client = new \Aws\S3\S3Client([
                'credentials' => [
                    'key' => $uploadParams['credentials']['AccessKeyId'],
                    'secret' => $uploadParams['credentials']['SecretAccessKey'],
                    'token' => $uploadParams['credentials']['SessionToken'],
                ],
                'version' => 'latest',
                'region' => $slicedFile['region'],
            ]);

            $part1URL = $s3Client->putObject([
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'part001.gz',
                'Body' => fopen(__DIR__ . '/../../_data/escaping.csv', 'r+'),
            ])->get('ObjectURL');
        }

        // Second upload
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

        if ($slicedFile['provider'] === Client::FILE_PROVIDER_AZURE) {
            $uploadParams = $slicedFile['absUploadParams'];

            $blobClient = BlobClientFactory::createClientFromConnectionString(
                $uploadParams['absCredentials']['SASConnectionString']
            );

            $blobClient->createBlockBlob(
                $uploadParams['container'],
                $uploadParams['blobName'] . 'manifest',
                json_encode([
                    'entries' => [
                        [
                            'url' => $part1URL,
                        ],
                    ],
                ])
            );
        } else {
            $uploadParams = $slicedFile['uploadParams'];
            $s3Client = new \Aws\S3\S3Client([
                'credentials' => [
                    'key' => $uploadParams['credentials']['AccessKeyId'],
                    'secret' => $uploadParams['credentials']['SecretAccessKey'],
                    'token' => $uploadParams['credentials']['SessionToken'],
                ],
                'version' => 'latest',
                'region' => $slicedFile['region'],
            ]);

            $s3Client->putObject([
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'] . 'manifest',
                'Body' => json_encode([
                    'entries' => [
                        [
                            'url' => $part1URL,
                        ],
                    ],
                ]),
            ])->get('ObjectURL');
        }

        try {
            $this->_client->writeTableAsyncDirect($tableId, array(
                'dataFileId' => $slicedFile['id'],
                'withoutHeaders' => true,
                'delimiter' => '|',
            ));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.unauthorizedAccess', $e->getStringCode());
        }
    }
}
