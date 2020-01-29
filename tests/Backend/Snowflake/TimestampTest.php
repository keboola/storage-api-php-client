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

    /**
     * Originally this is WorkspacesUnloadTest::testCopyImport but only works in snowflake
     */
    public function testTimestampCopyImport()
    {
        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ));

        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null,
			\"update\" varchar
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));
        unset($db);
        // test timestamp
        $tmpWorkspaceFull = $workspaces->createWorkspace();
        $tmpWorkspaceDb = $this->getDbConnection($tmpWorkspaceFull['connection']);
        $workspaces->cloneIntoWorkspace($tmpWorkspaceFull['id'], [
            'input' => [
                [
                    'source' => $table['id'],
                    'destination' => 'timestamptestFull',
                ],
            ],
        ]);
        $data = $tmpWorkspaceDb->fetchAll('SELECT "_timestamp" FROM "timestamptestFull"');
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }
        unset($tmpWorkspaceDb);

        $db = $this->getDbConnection($connection);
        $db->query("truncate \"test.Languages3\"");
        $db->query("insert into \"test.Languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));

        $db->query("truncate table \"test.Languages3\"");
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"new_col\" varchar");
        $db->query("insert into \"test.Languages3\" values (1, 'cz', '1', null), (3, 'sk', '1', 'newValue');");

        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
            'incremental' => true,
        ));
        unset($db);

        $tmpWorkspaceInc = $workspaces->createWorkspace();
        $tmpWorkspaceDb = $this->getDbConnection($tmpWorkspaceInc['connection']);
        $workspaces->cloneIntoWorkspace($tmpWorkspaceInc['id'], [
            'input' => [
                [
                    'source' => $table['id'],
                    'destination' => 'timestamptestInc',
                ],
            ],
        ]);
        $data = $tmpWorkspaceDb->fetchAll('SELECT "_timestamp" FROM "timestamptestInc"');
        foreach ($data as $timestampRecord) {
            $this->assertNotNull($timestampRecord['_timestamp']);
        }
        unset($tmpWorkspaceDb);
    }
}
