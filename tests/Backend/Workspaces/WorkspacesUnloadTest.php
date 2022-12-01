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

class WorkspacesUnloadTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testTableCloneCaseSensitiveThrowsUserError(): void
    {
        $tokenData = $this->_client->verifyToken();
        if (in_array($tokenData['owner']['defaultBackend'], [
            self::BACKEND_REDSHIFT,
            self::BACKEND_SYNAPSE,
            self::BACKEND_EXASOL,
            self::BACKEND_BIGQUERY,
        ], true)) {
            $this->markTestSkipped('Test case-sensitivity columns name only for snowflake');
        }

        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages-case-sensitive', $importFile);

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        unset($backend);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('create table "test_Languages3" (
			"id" integer not null,
			"Name" varchar(10) not null
		);');

        $db->query("insert into \"test_Languages3\" (\"id\", \"Name\") values (1, 'cz'), (2, 'en');");

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
        unset($backend);

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query('create table "test_Languages3" (
			"Id" integer not null,
			"Name" varchar(10) not null
		);');
        $db->query("insert into \"test_Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
        ]);

        $expected = [
            ($connection['backend'] === parent::BACKEND_REDSHIFT) ? '"id","name"' : '"Id","Name"',
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
        unset($backend);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('create table "test_Languages3" (
			"_Id" integer not null,
			"Name" varchar(10) not null
		);');
        $db->query("insert into \"test_Languages3\" (\"_Id\", \"Name\") values (1, 'cz'), (2, 'en');");

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
        unset($backend);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('create table "test_Languages3" (
			"Id" integer not null,
			"Name" varchar(10) not null,
			"_update" varchar(10) not null 
		);');
        $db->query("insert into \"test_Languages3\" (\"Id\", \"Name\", \"_update\") values (1, 'cz', 'x'), (2, 'en', 'z');");

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
        $tokenData = $this->_client->verifyToken();
        $testViewLoad = in_array($tokenData['owner']['defaultBackend'], [self::BACKEND_SNOWFLAKE,], true);

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
        $backend->dropViewIfExists('test_Languages3_view');
        unset($backend);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('create table "test_Languages3" (
			"Id" integer not null,
			"Name" varchar(10) not null,
			"update" varchar(10)
		);');

        $db->query("insert into \"test_Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

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

        if ($testViewLoad) {
            // test same thing like with table but on view
            $db->query('create view "test_Languages3_view" as select * from "test_Languages3";');
            /** @var array{id:string} $tableView */
            $tableView = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
                'dataString' => 'Id,Name,update',
                'name' => 'languages_from_view',
                'primaryKey' => 'Id',
            ]);
            $this->_client->writeTableAsyncDirect($tableView['id'], [
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test_Languages3_view',
            ]);
            $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
                'format' => 'rfc',
            ]), 'imported data comparsion');
        }

        $db->query('truncate table "test_Languages3"');
        $db->query("insert into \"test_Languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

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

        $db->query('truncate table "test_Languages3"');
        $db->query('alter table "test_Languages3" ADD COLUMN "new_col" varchar(10)');
        $db->query("insert into \"test_Languages3\" values (1, 'cz', '1', null), (3, 'sk', '1', 'newValue');");

        $this->_client->writeTableAsyncDirect($table['id'], [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
            'incremental' => true,
        ]);

        $expected = [
            '"Id","Name","update","new_col"',
            '"1","cz","1",""',
            '"2","en","",""',
            '"3","sk","1","newValue"',
        ];
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
            'format' => 'rfc',
        ]), 'new  column added');
    }
}
