<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class UnderscoreTimestampTest extends WorkspacesTestCase
{
    private $workspaces;

    public function setUp()
    {
        parent::setUp();

        $this->workspaces = new Workspaces($this->_client);
    }

    public function testTableCreateAndImport()
    {
        $workspace = $this->workspaces->createWorkspace();

        //table create
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);

        // table incremental load
        $this->_client->writeTable(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'incremental' => true,
            ]
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(10, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);

        // table full load
        $this->_client->writeTable(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'incremental' => false,
            ]
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);
    }

    public function testTableCreateFromWorkspace()
    {
        $workspace = $this->workspaces->createWorkspace();

        //table create
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);

        $this->workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages2',
                ]
            ],
        ]);

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableColumn('languages2', '_timestamp');
        unset($backend);

        // table create from workspace
        $table2Id = $this->_client->createTableAsyncDirect(
            $this->getTestBucketId(self::STAGE_IN),
            array(
            'name' => 'languages2',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'languages2',
            )
        );

        $table = $this->_client->getTable($table2Id);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($table2Id, $workspace);
    }

    public function testTableImportFromWorkspace()
    {
        $workspace = $this->workspaces->createWorkspace();

        //table create
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);

        $this->workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages2',
                ]
            ],
        ]);

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableColumn('languages2', '_timestamp');
        unset($backend);

        $this->_client->writeTableAsyncDirect(
            $tableId,
            [
                'incremental' => false,
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'languages2',
            ]
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);
    }

    public function testTableImportFromWorkspaceIncremental()
    {
        $workspace = $this->workspaces->createWorkspace();

        //table create
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(5, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);

        $this->workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages2',
                ]
            ],
        ]);

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableColumn('languages2', '_timestamp');
        unset($backend);

        $this->_client->writeTableAsyncDirect(
            $tableId,
            [
                'incremental' => true,
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'languages2',
            ]
        );

        $table = $this->_client->getTable($tableId);
        $this->assertSame(10, $table['rowsCount']);

        $this->checkTimestamp($tableId, $workspace);
    }

    private function checkTimestamp($tableStringId, array $workspace)
    {
        $this->workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableStringId,
                    'destination' => 'timestampCheck',
                ]
            ],
        ]);

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $backend->fetchAll('timestampCheck');

        $timestampColumnIndex = array_search('_timestamp', $backend->getTableColumns('timestampCheck'));
        $this->assertNotFalse($timestampColumnIndex);

        foreach ($data as $line) {
            $this->assertNotEmpty($line[$timestampColumnIndex]);
        }

        unset($backend);
    }
}
