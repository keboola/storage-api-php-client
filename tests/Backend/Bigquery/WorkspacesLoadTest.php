<?php

namespace Keboola\Test\Backend\Bigquery;

use Google\Cloud\BigQuery\Table;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\BigqueryWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Throwable;

class WorkspacesLoadTest extends ParallelWorkspacesTestCase
{
    public function testTableLoadAsView(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        assert($backend instanceof BigqueryWorkspaceBackend);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile)
        );

        // test loading alias in IM not supported
        $aliasId = $this->_client->createAliasTable($bucketId, $tableId, 'languages-alias');

        $options = [
            'input' => [
                [
                    'source' => $aliasId,
                    'destination' => 'languages',
                    'useView' => true,
                ],
            ],
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Must throw exception as load alias as view is not supported');
        } catch (ClientException $e) {
            $this->assertSame('workspace.loadRequestLogicalException', $e->getStringCode());
            $this->assertSame(
                'View load is not supported, only table can be loaded using views, alias of table supplied. Use read-only storage instead or copy input mapping if supported.',
                $e->getMessage()
            );
        } catch (Throwable $e) {
            $this->fail('Must throw ClientException as load alias as view is not supported. ' . $e->getMessage());
        }
        // drop alias as source table is modified later
        $this->_client->dropTable($aliasId);

        // test loading regular table as view
        $options['input'][0]['source'] = $tableId;
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $dataset = $backend->getDataset();
        $tables = iterator_to_array($dataset->tables());

        $this->assertCount(1, $tables);
        $this->assertSame('VIEW', $tables[0]->info()['type']);
        $this->assertCount(5, $backend->fetchAll('languages'));
        $this->assertColumns($dataset->table('languages'), ['id', 'name', '_timestamp']);

        // test if view select works after column add
        $this->_client->addTableColumn($tableId, 'newGuy');
        $this->assertCount(5, $backend->fetchAll('languages'));
        // new column is not visible in view
        $this->assertColumns($dataset->table('languages'), ['id', 'name', '_timestamp']);

        // test that does work after column remove
        $this->_client->deleteTableColumn($tableId, 'name');
        $this->assertCount(5, $backend->fetchAll('languages'));
        $this->assertColumns($dataset->table('languages'), ['id', 'name', '_timestamp']);
        $backend->dropView('languages');

        // clear and create table again
        $this->_client->dropTable($tableId);
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile)
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // test preserve load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Must throw exception view exists');
        } catch (ClientException $e) {
            self::assertEquals('Table languages already exists in workspace', $e->getMessage());
        }

        // test preserve load with overwrite
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'useView' => true,
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        self::assertCount(5, $backend->fetchAll('languages'));
        $this->assertColumns($dataset->table('languages'), ['id', 'name', '_timestamp']);

        // test workspace load incremental to view
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'incremental' => true,
                    'useView' => true,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Incremental load to view cannot work.');
        } catch (ClientException $e) {
            self::assertSame(
                'Incremental load for table "languages" can\'t be used when using view.',
                $e->getMessage()
            );
        }

        // do incremental load from file to source table
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            ['incremental' => true]
        );
        // test view is still working
        self::assertCount(10, $backend->fetchAll('languages'));
        $this->assertColumns($dataset->table('languages'), ['id', 'name', '_timestamp']);

        // test drop table
        $this->_client->dropTable($tableId);
        $schemaRef = $backend->getSchemaReflection();
        self::assertCount(0, $schemaRef->getTablesNames());
        // view is still in workspace but not working
        self::assertCount(1, $schemaRef->getViewsNames());
        try {
            $backend->fetchAll('languages');
            $this->fail('View should not work after table drop');
        } catch (Throwable $e) {
            $this->assertStringContainsString('Not found', $e->getMessage());
        }
    }

    private function assertColumns(Table $table, array $expectedColumns): void
    {
        $table->reload();
        $this->assertSame(
            $expectedColumns,
            array_map(fn(array $i) => $i['name'], $table->info()['schema']['fields'])
        );
    }
}
