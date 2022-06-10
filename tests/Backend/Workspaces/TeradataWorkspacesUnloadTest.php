<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class TeradataWorkspacesUnloadTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;

    public function testCreateTableFromWorkspace(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        unset($backend);

        $connection = $workspace['connection'];

        $db = $this->getDbConnection($connection);

        $db->query('CREATE TABLE "test_Languages3" (
			"Id" integer NOT NULL,
			"Name" varchar(10) NOT NULL
		);');
        $db->query('INSERT INTO "test_Languages3" ("id", "Name") VALUES (1, \'cz\');');
        $db->query('INSERT INTO "test_Languages3" ("id", "Name") VALUES (2, \'en\');');

        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
            'name' => 'languages3',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_Languages3',
        ]);

        $expected = [
            '"Id","Name"',
            '"          1","cz"',
            '"          2","en"',
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

        $db->query('CREATE TABLE "test_Languages3" (
			"_Id" integer NOT NULL,
			"Name" varchar(10) NOT NULL
		);');
        $db->query('INSERT INTO "test_Languages3" ("_Id", "Name") VALUES (1, \'cz\');');
        $db->query('INSERT INTO "test_Languages3" ("_Id", "Name") VALUES (2, \'en\');');
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
        $this->markTestSkipped('TODO: deduplication');
        //// create workspace and source table in workspace
        //$workspace = $this->initTestWorkspace();
        //
        //$table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
        //    'dataString' => 'Id,Name',
        //    'name' => 'languages',
        //    'primaryKey' => 'Id',
        //]);
        //
        //try {
        //    $this->_client->writeTableAsyncDirect($table['id'], [
        //        'dataWorkspaceId' => $workspace['id'],
        //        'dataTableName' => 'thisTableDoesNotExist',
        //    ]);
        //    $this->fail('Table should not be imported');
        //} catch (ClientException $e) {
        //    $this->assertEquals('storage.tableNotFound', $e->getStringCode());
        //    $this->assertEquals(
        //        sprintf(
        //            'Table "thisTableDoesNotExist" not found in schema "%s"',
        //            $workspace['connection']['schema']
        //        ),
        //        $e->getMessage()
        //    );
        //}
        //
        //try {
        //    $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), [
        //        'name' => 'thisTableDoesNotExist',
        //        'dataWorkspaceId' => $workspace['id'],
        //        'dataTableName' => 'thisTableDoesNotExist',
        //    ]);
        //    $this->fail('Table should not be imported');
        //} catch (ClientException $e) {
        //    $this->assertEquals('storage.tableNotFound', $e->getStringCode());
        //    $this->assertEquals(
        //        sprintf(
        //            'Table "thisTableDoesNotExist" not found in schema "%s"',
        //            $workspace['connection']['schema']
        //        ),
        //        $e->getMessage()
        //    );
        //}
    }

    public function testImportFromWorkspaceWithInvalidColumnNames(): void
    {
        $this->markTestSkipped('TODO: deduplication');
        //// create workspace and source table in workspace
        //$workspace = $this->initTestWorkspace();
        //
        //$backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        //$backend->dropTableIfExists('test_Languages3');
        //unset($backend);
        //
        //$db = $this->getDbConnection($workspace['connection']);
        //
        //$db->query('CREATE TABLE "test_Languages3" (
        //"Id" integer NOT NULL,
        //"Name" varchar(10) NOT NULL,
        //"_update" varchar(10) NOT NULL
        //);');
        //$db->query('INSERT INTO "test_Languages3" ("Id", "Name", "_update") VALUES (1, \'cz\', \'x\');');
        //$db->query('INSERT INTO "test_Languages3" ("Id", "Name", "_update") VALUES (2, \'en\', \'z\');');
        //
        //$table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
        //    'dataString' => 'Id,Name',
        //    'name' => 'languages',
        //    'primaryKey' => 'Id',
        //]);

        // incremental not implemented
        //try {
        //    $this->_client->writeTableAsyncDirect($table['id'], array(
        //        'dataWorkspaceId' => $workspace['id'],
        //        'dataTableName' => 'test_Languages3',
        //        'incremental' => true,
        //    ));
        //    $this->fail('Table should not be imported');
        //} catch (ClientException $e) {
        //    $this->assertEquals('storage.invalidColumns', $e->getStringCode());
        //    $this->assertStringContainsString('_update', $e->getMessage());
        //}
    }

    /**
     * TODO should be replaced with testCopyImport after deduplication works
     */
    public function testCopyImportSimple(): void
    {
        /** @var array{id:string} $table */
        $table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
        ]);

        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_Languages3');
        unset($backend);

        $db = $this->getDbConnection($workspace['connection']);

        $db->query('CREATE TABLE "test_Languages3" (
			"Id" integer NOT NULL,
			"Name" varchar(10) NOT NULL,
			"update" varchar(10)
		);');

        $db->query('INSERT INTO "test_Languages3" ("Id", "Name") VALUES (1, \'cz\');');
        $db->query('INSERT INTO "test_Languages3" ("Id", "Name") VALUES (2, \'en\');');

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
    }

    public function testCopyImport(): void
    {
        $this->markTestSkipped('TODO: deduplication');
        //$table = $this->_client->apiPost('buckets/' . $this->getTestBucketId(self::STAGE_IN) . '/tables', [
        //    'dataString' => 'Id,Name,update',
        //    'name' => 'languages',
        //    'primaryKey' => 'Id',
        //]);
        //
        //// create workspace and source table in workspace
        //$workspace = $this->initTestWorkspace();
        //
        //$backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        //$backend->dropTableIfExists('test_Languages3');
        //unset($backend);
        //
        //$db = $this->getDbConnection($workspace['connection']);
        //
        //$db->query('CREATE TABLE "test_Languages3" (
        //"Id" integer NOT NULL,
        //"Name" varchar(10) NOT NULL,
        //"update" varchar(10)
        //);');
        //
        //$db->query('INSERT INTO "test_Languages3" ("Id", "Name") VALUES (1, \'cz\');');
        //$db->query('INSERT INTO "test_Languages3" ("Id", "Name") VALUES (2, \'en\');');
        //
        //$this->_client->writeTableAsyncDirect($table['id'], [
        //    'dataWorkspaceId' => $workspace['id'],
        //    'dataTableName' => 'test_Languages3',
        //]);
        //
        //$expected = [
        //    '"Id","Name","update"',
        //    '"1","cz",""',
        //    '"2","en",""',
        //];
        //
        //$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], [
        //    'format' => 'rfc',
        //]), 'imported data comparsion');
        //
        //$db->query('truncate table "test_Languages3"');
        //$db->query('INSERT INTO "test_Languages3" VALUES (1, \'cz\', \'1\');');
        //$db->query('INSERT INTO "test_Languages3" VALUES (3, \'sk\', \'1\');');

        // incremental not implemented
        //$this->_client->writeTableAsyncDirect($table['id'], array(
        //    'dataWorkspaceId' => $workspace['id'],
        //    'dataTableName' => 'test_Languages3',
        //    'incremental' => true,
        //));
        //
        //$expected = array(
        //    '"Id","Name","update"',
        //    '"1","cz","1"',
        //    '"2","en",""',
        //    '"3","sk","1"',
        //);
        //$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
        //    'format' => 'rfc',
        //)), 'previously null column updated');
        //
        //$db->query("truncate table \"test_Languages3\"");
        //$db->query('alter table "test_Languages3" ADD COLUMN "new_col" varchar(10)');
        //$db->query('insert into "test_Languages3" values (1, \'cz\', \'1\', null);');
        //$db->query('insert into "test_Languages3" values (3, \'sk\', \'1\', \'newValue\');');
        //
        //$this->_client->writeTableAsyncDirect($table['id'], array(
        //    'dataWorkspaceId' => $workspace['id'],
        //    'dataTableName' => 'test_Languages3',
        //    'incremental' => true,
        //));
        //
        //$expected = array(
        //    '"Id","Name","update","new_col"',
        //    '"1","cz","1",""',
        //    '"2","en","",""',
        //    '"3","sk","1","newValue"',
        //);
        //$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
        //    'format' => 'rfc',
        //)), 'new  column added');
    }
}
