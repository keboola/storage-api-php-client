<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;

class WorkspacesUnloadTest extends FileWorkspaceTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->deleteAllWorkspaces();
    }

    public function testCreateTableFromWorkspace(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);
        $backend = new Abs($workspace['connection']);

        $tableEmptyCsv = __DIR__ . '/../../_data/languages.empty.csv';
        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1CsvIncremental = __DIR__ . '/../../_data/languages.increment.csv';

        // load table 1
        $this->loadTable(
            $table1Csv,
            'languages',
            'languagesLoaded',
            $workspaces,
            (int) $workspace['id'],
        );
        // load table 1 incremental
        $this->loadTable(
            $table1CsvIncremental,
            'languagesInc',
            'languagesLoadedIncremental',
            $workspaces,
            (int) $workspace['id'],
            true,
        );
        // upload csv
        $backend->uploadFile($table1Csv, 'languages.csv');

        // create table from workspace no columns
        try {
            $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
                'name' => 'languagesUnload',
                'dataWorkspaceId' => $workspace['id'],
                'dataObject' => 'languagesLoaded',
            ]);
            $this->fail('missing columns exceptions should be thrown.');
        } catch (ClientException $e) {
            $this->assertEquals('columns must be set for file workspaces.', $e->getMessage());
        }

        // create table from workspace slices
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languagesUnload',
            'dataWorkspaceId' => $workspace['id'],
            'dataObject' => 'languagesLoaded/',
            'columns' => ['id', 'name'],
        ]);

        $this->assertLinesEqualsSorted(
            file_get_contents($table1Csv),
            $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            ),
            'imported data comparsion',
        );

        // create table from workspace manifest
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languagesUnloadManifest',
            'dataWorkspaceId' => $workspace['id'],
            'dataObject' => 'languagesLoadedmanifest',
            'columns' => ['id', 'name'],
        ]);

        $this->assertLinesEqualsSorted(
            file_get_contents($table1Csv),
            $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            ),
            'imported data comparsion',
        );

        // create table from workspace csv file
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languagesCsv',
            'dataWorkspaceId' => $workspace['id'],
            'dataObject' => 'languages.csv',
            'columns' => ['id', 'name'],
        ]);

        $this->assertLinesEqualsSorted(
            file_get_contents($table1Csv),
            $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            ),
            'imported data comparsion',
        );

        // clear table
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($tableEmptyCsv),
            [],
        );
        $this->assertLinesEqualsSorted(
            file_get_contents($tableEmptyCsv),
            $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            ),
            'imported data comparsion',
        );

        // write to table from workspace
        $this->_client->writeTableAsyncDirect(
            $tableId,
            [
                'dataWorkspaceId' => $workspace['id'],
                'dataObject' => 'languagesLoaded/',
                'columns' => ['id', 'name'],
            ],
        );

        $this->assertLinesEqualsSorted(
            file_get_contents($table1Csv),
            $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            ),
            'imported data comparsion',
        );

        // write to table incremental
        $this->_client->writeTableAsyncDirect(
            $tableId,
            [
                'dataWorkspaceId' => $workspace['id'],
                'dataObject' => 'languagesLoadedIncremental/',
                'columns' => ['id', 'name'],
                'incremental' => true,
            ],
        );

        $this->assertCount(
            10,
            explode("\n", $this->_client->getTableDataPreview(
                $tableId,
                [
                    'format' => 'rfc',
                ],
            )),
            'imported data comparsion',
        );
    }

    /**
     * @param string $csvPath
     * @param string $tableName
     * @param string $unloadDestination
     * @param Workspaces $workspaces
     * @param int $workspaceId
     * @param bool $preserve
     */
    private function loadTable(
        $csvPath,
        $tableName,
        $unloadDestination,
        Workspaces $workspaces,
        $workspaceId,
        $preserve = false
    ) {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            $tableName,
            new CsvFile($csvPath),
        );
        $workspaces->loadWorkspaceData($workspaceId, [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $unloadDestination,
                ],
            ],
            'preserve' => $preserve,
        ]);

        return $tableId;
    }
}
