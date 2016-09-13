<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;


class WorkspacesSnowflakeTest extends WorkspacesTestCase {

    public function testTransientTAbles() {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages-rs',
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

}