<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class TimestampTest extends WorkspacesTestCase
{
    /**
     * Originally this is ImportExportCommonTest::testTableAsyncImportExport but only works in snowflake
     */
    public function testTimestampCSVImportAsync()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $createTableOptions = [];

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-3',
            $importFile,
            $createTableOptions
        );

        $this->_client->writeTableAsync($tableId, $importFile);

        $workspaces = new Workspaces($this->_client);
        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestFull',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestFull', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }

        // incremental
        $this->_client->writeTableAsync($tableId, $importFile, [
            'incremental' => true,
        ]);

        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestInc',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestInc', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }
    }

    /**
     * Originally this is ImportExportCommonTest::testTableImportExport but only works in snowflake
     */
    public function testTimestampCSVImportSync()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $createTableOptions = [];

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-2',
            $importFile,
            $createTableOptions
        );

        $this->_client->writeTable($tableId, $importFile);

        $workspaces = new Workspaces($this->_client);
        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestFull',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestFull', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }

        // incremental
        $this->_client->writeTable($tableId, $importFile, [
            'incremental' => true,
        ]);

        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestInc',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestInc', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }
    }

    /**
     * Originally this is SlicedImportsTest::testSlicedImportSingleFile but only works in snowflake
     */
    public function testTimestampSlicedImport()
    {
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions
            ->setFileName('languages_')
            ->setIsSliced(true)
            ->setIsEncrypted(false);
        $slicedFile = $this->_client->prepareFileUpload($uploadOptions);

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
            'Key' => $uploadParams['key'] . 'part001.csv',
            'Body' => fopen(__DIR__ . '/../../_data/languages.no-headers.csv', 'r+'),
        ])->get('ObjectURL');

        $s3Client->putObject([
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode([
                'entries' => [
                    [
                        'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.csv',
                    ],
                ],
            ]),
        ])->get('ObjectURL');

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'entries',
            new CsvFile(__DIR__ . '/../../_data/languages.not-normalized-column-names.csv')
        );
        $this->_client->deleteTableRows($tableId);
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $slicedFile['id'],
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => [
                'language-id',
                'language-name',
            ],
        ]);

        $workspaces = new Workspaces($this->_client);
        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestFull',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestFull', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }

        // incremental
        $this->_client->writeTableAsyncDirect($tableId, [
            'dataFileId' => $slicedFile['id'],
            'incremental' => true,
            'delimiter' => ',',
            'enclosure' => '"',
            'escapedBy' => '',
            'columns' => [
                'language-id',
                'language-name',
            ],
        ]);

        $tmpWorkspace = $workspaces->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($tmpWorkspace);
        $workspaces->cloneIntoWorkspace($tmpWorkspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'timestampTestInc',
                ],
            ],
        ]);
        $data = $backend->fetchAll('timestampTestInc', \PDO::FETCH_ASSOC);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }
    }
}
