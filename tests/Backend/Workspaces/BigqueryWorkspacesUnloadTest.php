<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class BigqueryWorkspacesUnloadTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testTableCloneCaseSensitiveThrowsUserError(): void
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages-case-sensitive', $importFile);

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        $backend->createTable('test_Languages3', [
            'id' => 'integer',
            'Name' => 'string',
        ]);
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` (`id`, `Name`) VALUES (1, \'cz\'), (2, \'en\');',
            $workspace['connection']['schema']
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Some columns are missing in the csv file. Missing columns: name. Expected columns: id,name. ');

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
            'incremental' => true,
        ]);
    }

    public function testCreateTableFromWorkspace(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        $backend->createTable('test_Languages3', [
            'Id' => 'integer',
            'Name' => 'string',
        ]);
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` (`Id`, `Name`) VALUES (1, \'cz\'), (2, \'en\');',
            $workspace['connection']['schema']
        ));
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
        ]);

        $expected = [
            '"Id","Name"',
            '"1","cz"',
            '"2","en"',
        ];

        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($tableId, [
            'format' => 'rfc',
        ]), 'imported data comparsion');
    }

    public function testCreateTableFromWorkspaceWithInvalidColumnNames(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        $backend->createTable('test_Languages3', [
            '_Id' => 'integer',
            'Name' => 'string',
        ]);
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` (`_Id`, `Name`) VALUES (1, \'cz\'), (2, \'en\');',
            $workspace['connection']['schema']
        ));

        try {
            $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test_Languages3',
            ]);
            $this->fail('Table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
            $this->assertStringContainsString('_id', strtolower($e->getMessage())); // RS is case insensitive, others are not
        }
    }

    public function testImportFromWorkspaceWithInvalidTableNames(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        /** @var array{id:string} $table */
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => 'Id,Name',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ]);

        try {
            $this->_client->writeTableAsyncDirect($table['id'], [
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'thisTableDoesNotExist',
            ]);
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tableNotFound', $e->getStringCode());
            $this->assertEquals(
                sprintf(
                    'Table "thisTableDoesNotExist" not found in schema "%s"',
                    $workspace['connection']['schema']
                ),
                $e->getMessage()
            );
        }

        try {
            $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
                'name' => 'thisTableDoesNotExist',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'thisTableDoesNotExist',
            ]);
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tableNotFound', $e->getStringCode());
            $this->assertEquals(
                sprintf(
                    'Table "thisTableDoesNotExist" not found in schema "%s"',
                    $workspace['connection']['schema']
                ),
                $e->getMessage()
            );
        }
    }

    public function testImportFromWorkspaceWithInvalidColumnNames(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        $backend->createTable('test_Languages3', [
            'Id' => 'integer',
            'Name' => 'string',
            '_update' => 'string',
        ]);
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` (`Id`, `Name`, `_update`) VALUES (1, \'cz\', \'x\'), (2, \'en\', \'z\');',
            $workspace['connection']['schema']
        ));

        /** @var array{id:string} $table */
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => 'Id,Name',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ]);

        try {
            $this->_client->writeTableAsyncDirect($table['id'], [
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test_Languages3',
                'incremental' => true,
            ]);
            $this->fail('Table should not be imported');
        } catch (ClientException $e) {
            $this->assertEquals('storage.invalidColumns', $e->getStringCode());
            $this->assertStringContainsString('_update', $e->getMessage());
        }
    }

    public function testCopyImport(): void
    {
        /** @var array{id:string} $table */
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ]);

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        $backend->createTable('test_Languages3', [
            'Id' => 'integer',
            'Name' => 'string',
            'update' => 'string',
        ]);
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` (`Id`, `Name`) VALUES (1, \'cz\'), (2, \'en\');',
            $workspace['connection']['schema']
        ));

        $this->_client->writeTableAsyncDirect($table['id'], [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
        ]);

        $expected = [
            '"Id","Name","update"',
            '"1","cz",""',
            '"2","en",""',
        ];

        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
            'format' => 'rfc',
        ]), 'imported data comparsion');

        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'TRUNCATE TABLE %s.`test_Languages3`',
            $workspace['connection']['schema']
        ));
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` VALUES (1, \'cz\', \'1\'), (3, \'sk\', \'1\');',
            $workspace['connection']['schema']
        ));

        $this->_client->writeTableAsyncDirect($table['id'], [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
            'incremental' => true,
        ]);

        $expected = [
            '"Id","Name","update"',
            '"1","cz","1"',
            '"2","en",""',
            '"3","sk","1"',
        ];
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
            'format' => 'rfc',
        ]), 'previously null column updated');

        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'TRUNCATE TABLE %s.`test_Languages3`',
            $workspace['connection']['schema']
        ));
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'ALTER TABLE %s.`test_Languages3` ADD COLUMN `new_col` string(10)',
            $workspace['connection']['schema']
        ));
        $backend->executeQuery(sprintf(
        /** @lang BigQuery */
            'INSERT INTO %s.`test_Languages3` VALUES (1, \'cz\', \'1\', NULL), (3, \'sk\', \'1\', \'newValue\');',
            $workspace['connection']['schema']
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('During the import of typed tables new columns can\'t be added. Extra columns found: "new_col".');
        $this->_client->writeTableAsyncDirect($table['id'], [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
            'incremental' => true,
        ]);

//        $expected = [
//            '"Id","Name","update","new_col"',
//            '"1","cz","1",""',
//            '"2","en","",""',
//            '"3","sk","1","newValue"',
//        ];
//        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
//            'format' => 'rfc',
//        ]), 'new  column added');
    }
}
