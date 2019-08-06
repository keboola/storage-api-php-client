<?php


namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class MetadataFromRedshiftWorkspaceTest extends WorkspacesTestCase
{
    public function testCreateTableFromWorkspace()
    {
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => "redshift"]);
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.metadata_columns\" (
                    \"string\" varchar(16) not null default 'string',
                    \"char\" char null,
                    \"smallint\" smallint null,
                    \"integer\" integer not null default 4,
                    \"bigint\" bigint null,
                    \"decimal\" decimal(10,3) not null default 234.123,
                    \"real\" real null,
                    \"double\" double precision null,
                    \"boolean\" boolean not null default true,
                    \"date\" date not null default current_date,
                    \"timestamp\" timestamp not null default sysdate,
                    \"timestamptz\" timestamptz not null default sysdate 
                );");
        // create table from workspace
        $tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
            'name' => 'metadata_columns',
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.metadata_columns',
        ));
        $expectedStringMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '16',
            'KBC.datatype.default' => 'string',
        ];
        $expectedCharMetadata = [
            'KBC.datatype.type' => 'BPCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
        ];
        $expectedSmallintMetadata = [
            'KBC.datatype.type' => 'INT2',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
        ];
        $expectedIntegerMetadata = [
            'KBC.datatype.type' => 'INT4',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
            'KBC.datatype.default' => '4',
        ];
        $expectedBigIntMetadata = [
            'KBC.datatype.type' => 'INT8',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'INTEGER',
        ];
        $expectedDecimalMetadata = [
            'KBC.datatype.type' => 'NUMERIC',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'NUMERIC',
            'KBC.datatype.length' => '10,3',
            'KBC.datatype.default' => '234.123',
        ];
        $expectedRealMetadata = [
            'KBC.datatype.type' => 'FLOAT4',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedDoubleMetadata = [
            'KBC.datatype.type' => 'FLOAT8',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'FLOAT',
        ];
        $expectedBooleanMetadata = [
            'KBC.datatype.type' => 'BOOL',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'BOOLEAN',
            'KBC.datatype.default' => 'true',
        ];
        $expectedDateMetadata = [
            'KBC.datatype.type' => 'DATE',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'DATE',
            'KBC.datatype.default' => '(\'now\'::text)::date',
        ];
        $expectedTimestampMetadata = [
            'KBC.datatype.type' => 'TIMESTAMP',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => '(\'now\'::text)::timestamp without time zone',
        ];
        $expectedTimestamptzMetadata = [
            'KBC.datatype.type' => 'TIMESTAMPTZ',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'TIMESTAMP',
            'KBC.datatype.default' => '(\'now\'::text)::timestamp without time zone',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($tableId);

        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('string', $table['columnMetadata']);
        $this->assertMetadata($expectedStringMetadata, $table['columnMetadata']['string']);
        $this->assertArrayHasKey('char', $table['columnMetadata']);
        $this->assertMetadata($expectedCharMetadata, $table['columnMetadata']['char']);
        $this->assertArrayHasKey('smallint', $table['columnMetadata']);
        $this->assertMetadata($expectedSmallintMetadata, $table['columnMetadata']['smallint']);
        $this->assertArrayHasKey('integer', $table['columnMetadata']);
        $this->assertMetadata($expectedIntegerMetadata, $table['columnMetadata']['integer']);
        $this->assertArrayHasKey('bigint', $table['columnMetadata']);
        $this->assertMetadata($expectedBigIntMetadata, $table['columnMetadata']['bigint']);
        $this->assertArrayHasKey('decimal', $table['columnMetadata']);
        $this->assertMetadata($expectedDecimalMetadata, $table['columnMetadata']['decimal']);
        $this->assertArrayHasKey('real', $table['columnMetadata']);
        $this->assertMetadata($expectedRealMetadata, $table['columnMetadata']['real']);
        $this->assertArrayHasKey('double', $table['columnMetadata']);
        $this->assertMetadata($expectedDoubleMetadata, $table['columnMetadata']['double']);
        $this->assertArrayHasKey('boolean', $table['columnMetadata']);
        $this->assertMetadata($expectedBooleanMetadata, $table['columnMetadata']['boolean']);
        $this->assertArrayHasKey('date', $table['columnMetadata']);
        $this->assertMetadata($expectedDateMetadata, $table['columnMetadata']['date']);
        $this->assertArrayHasKey('timestamp', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestampMetadata, $table['columnMetadata']['timestamp']);
        $this->assertArrayHasKey('timestamptz', $table['columnMetadata']);
        $this->assertMetadata($expectedTimestamptzMetadata, $table['columnMetadata']['timestamptz']);
    }

    public function testCopyImport()
    {
        $table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
            'dataString' => 'Id,Name,update',
            'name' => 'languages',
            'primaryKey' => 'Id',
        ));
        // create workspace and source table in workspace
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace(["backend" => "redshift"]);
        $connection = $workspace['connection'];
        $db = $this->getDbConnection($connection);
        $db->query("create table \"test.Languages3\" (
                \"Id\" integer not null,
                \"Name\" varchar not null default 'honza',
                \"update\" varchar
            );");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");
        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages3',
        ));
        $expected = array(
            '"Id","Name","update"',
            '"1","cz",""',
            '"2","en",""',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'imported data comparsion');
        // check the created metadata
        $expectedIdMetadata = [
            'KBC.datatype.type' => 'INT4',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'INTEGER',
        ];
        $expectedNameMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
            'KBC.datatype.default' => 'honza',
        ];
        $expectedUpdateMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '256',
        ];
        // check that the new table has the correct metadata
        $table = $this->_client->getTable($table['id']);
        
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey('id', $table['columnMetadata']);
        $this->assertMetadata($expectedIdMetadata, $table['columnMetadata']['id']);
        $this->assertArrayHasKey('name', $table['columnMetadata']);
        $this->assertMetadata($expectedNameMetadata, $table['columnMetadata']['name']);
        $this->assertArrayHasKey('update', $table['columnMetadata']);
        $this->assertMetadata($expectedUpdateMetadata, $table['columnMetadata']['update']);
        $db->query("truncate table \"test.Languages3\"");
        $db->query("alter table \"test.Languages3\" ADD COLUMN \"new_col\" varchar(64)");
        $db->query("alter table \"test.Languages3\" RENAME TO \"test.Languages4\"");
        $db->query("insert into \"test.Languages4\" values " .
            "(1, 'cz', '1', null)," .
            " (3, 'sk', '1', 'newValue')," .
            " (4, 'jp','1', 'test');");
        $this->_client->writeTableAsyncDirect($table['id'], array(
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test.Languages4',
            'incremental' => true,
        ));
        $expected = array(
            '"Id","Name","update","new_col"',
            '"1","cz","1",""',
            '"2","en","",""',
            '"3","sk","1","newValue"',
            '"4","jp","1","test"',
        );
        $this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->getTableDataPreview($table['id'], array(
            'format' => 'rfc',
        )), 'new  column added');
        $expectedNewColMetadata = [
            'KBC.datatype.type' => 'VARCHAR',
            'KBC.datatype.nullable' => '1',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.length' => '64',
        ];
        $table = $this->_client->getTable($table['id']);
        $this->assertEquals([], $table['metadata']);
        $this->assertArrayHasKey("id", $table['columnMetadata']);
        $this->assertArrayHasKey("name", $table['columnMetadata']);
        $this->assertArrayHasKey("update", $table['columnMetadata']);
        $this->assertArrayHasKey("new_col", $table['columnMetadata']);
        $this->assertMetadata($expectedNewColMetadata, $table['columnMetadata']['new_col']);
    }


    private function assertMetadata($expectedKeyValues, $metadata)
    {
        $this->assertEquals(count($expectedKeyValues), count($metadata));
        foreach ($metadata as $data) {
            $this->assertArrayHasKey("key", $data);
            $this->assertArrayHasKey("value", $data);
            $this->assertEquals($expectedKeyValues[$data['key']], $data['value']);
            $this->assertArrayHasKey("provider", $data);
            $this->assertArrayHasKey("timestamp", $data);
            $this->assertRegExp(self::ISO8601_REGEXP, $data['timestamp']);
            $this->assertEquals('storage', $data['provider']);
        }
    }
}
