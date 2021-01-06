<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class TimestampTest extends ParallelWorkspacesTestCase
{
    const TIMESTAMP_FORMAT = '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/';

    /**
     * Originally this is ImportExportCommonTest::testTableAsyncImportExport but only works in snowflake
     */
    public function testTimestampCSVImportAsync()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        // count - header
        $count = iterator_count($importFile) - 1;

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-3',
            $importFile,
            []
        );

        $this->_client->writeTableAsync($tableId, $importFile);

        $this->assertDataInTable($tableId, 'timestampCSVImportAsyncFull', $count);

        // incremental
        $this->_client->writeTableAsync($tableId, $importFile, [
            'incremental' => true,
        ]);

        $this->assertDataInTable($tableId, 'timestampCSVImportAsyncInc', $count + $count);
    }

    /**
     * Originally this is ImportExportCommonTest::testTableImportExport but only works in snowflake
     */
    public function testTimestampCSVImportSync()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        // count - header
        $count = iterator_count($importFile) - 1;

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-2',
            $importFile,
            []
        );

        $this->_client->writeTable($tableId, $importFile);

        $this->assertDataInTable($tableId, 'timestampCSVImportSyncFull', $count);

        // incremental
        $this->_client->writeTable($tableId, $importFile, [
            'incremental' => true,
        ]);

        $this->assertDataInTable($tableId, 'timestampCSVImportSyncInc', $count + $count);
    }

    /**
     * Originally this is SlicedImportsTest::testSlicedImportSingleFile but only works in snowflake
     */
    public function testTimestampSlicedImport()
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
            ],
        ]);

        $count = iterator_count(new CsvFile(__DIR__ . '/../../_data/languages.no-headers.csv'));

        $this->assertDataInTable($tableId, 'timestampSlicedImportFull', $count);

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
            ],
        ]);

        $this->assertDataInTable($tableId, 'timestampSlicedImportInc', $count + $count);
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

        $workspaceApi = new Workspaces($this->workspaceSapiClient);

        // create workspace and source table in workspace
        $workspace = $workspaceApi->createWorkspace();
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null,
			\"update\" varchar
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        // copy data from workspace
        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));
        unset($db);
        // test timestamp
        $this->assertDataInTable($table['id'], 'timestampCopyImportFull', 2);

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

        $this->assertDataInTable($table['id'], 'timestampCopyImportInc', 3);
    }

    /**
     * @param string $tableId
     * @param string $workspaceTableName
     * @param int $expectedRows
     */
    private function assertDataInTable($tableId, $workspaceTableName, $expectedRows)
    {
        $workspaceApi = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaceApi->createWorkspace();
        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $workspaceApi->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $workspaceTableName,
                ],
            ],
        ]);
        $data = $backend->fetchAll($workspaceTableName, \PDO::FETCH_ASSOC);
        $this->assertCount($expectedRows, $data);
        foreach ($data as $timestampRecord) {
            $this->assertNotNull(
                $timestampRecord['_timestamp'],
                '_timestamp field must not be a null.'
            );
            $this->assertRegExp(
                self::TIMESTAMP_FORMAT,
                $timestampRecord['_timestamp'],
                '_timestamp has wrong pattern.'
            );
        }
    }
}
