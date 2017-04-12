<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesSnowflakeTest extends WorkspacesTestCase
{

    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->createWorkspace(["backend" => "redshift"]);
            $this->fail("should not be able to create WS for unsupported backend");
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.backendNotSupported");
        }
    }

    public function testStatementTimeout()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $this->assertGreaterThan(0, $workspace['statementTimeoutSeconds']);

        $db = $this->getDbConnection($workspace['connection']);

        $timeout = $db->fetchAll('SHOW PARAMETERS LIKE \'STATEMENT_TIMEOUT_IN_SECONDS\'')[0]['value'];
        $this->assertEquals($workspace['statementTimeoutSeconds'], $timeout);
    }

    public function testTransientTables()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
            new CsvFile($importFile)
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $tableId,
                    "destination" => "languages",
                ]
            ]
        ]);

        $db = $this->getDbConnection($workspace['connection']);

        // check if schema is transient
        $schemas = $db->fetchAll("SHOW SCHEMAS");

        $workspaceSchema = null;
        foreach ($schemas as $schema) {
            if ($schema['name'] === $workspace['connection']['schema']) {
                $workspaceSchema = $schema;
                break;
            }
        }

        $this->assertNotEmpty($workspaceSchema, 'schema not found');
        $this->assertEquals('TRANSIENT', $workspaceSchema['options']);

        $tables = $db->fetchAll("SHOW TABLES IN SCHEMA " . $db->quoteIdentifier($workspaceSchema['name']));
        $this->assertCount(1, $tables);
        
        $table = reset($tables);
        $this->assertEquals('languages', $table['name']);
        $this->assertEquals('TRANSIENT', $table['kind']);
    }


    public function testLoadedPrimaryKeys()
    {
        $primaries = ['Paid_Search_Engine_Account','Date','Paid_Search_Campaign','Paid_Search_Ad_ID','Site__DFA'];
        $pkTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            array(
                'primaryKey' => implode(",", $primaries),
            )
        );

        $mapping = [
            "source" => $pkTableId,
            "destination" => "languages-pk"
        ];

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping]]);

        $cols = $backend->describeTableColumns("languages-pk");
        $this->assertCount(6, $cols);
        $this->assertEquals("Paid_Search_Engine_Account", $cols[0]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[0]['type']);
        $this->assertEquals("Advertiser_ID", $cols[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[1]['type']);
        $this->assertEquals("Date", $cols[2]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[2]['type']);
        $this->assertEquals("Paid_Search_Campaign", $cols[3]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[3]['type']);
        $this->assertEquals("Paid_Search_Ad_ID", $cols[4]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[4]['type']);
        $this->assertEquals("Site__DFA", $cols[5]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[5]['type']);

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            "source" => $pkTableId,
            "destination" => "languages-pk-skipped",
            "columns" => ['Paid_Search_Engine_Account','Date'] // missing PK columns
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping2]]);

        $cols = $backend->describeTableColumns("languages-pk-skipped");
        $this->assertCount(2, $cols);
        $this->assertEquals("Paid_Search_Engine_Account", $cols[0]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[0]['type']);
        $this->assertEquals("Date", $cols[1]['name']);
        $this->assertEquals("VARCHAR(16777216)", $cols[1]['type']);
    }
}
