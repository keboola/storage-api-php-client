<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;


class WorkspacesRedshiftTest extends WorkspacesTestCase {

    public function testColumnCompression() {
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
                    "destination" => "languages-rs",
                    "datatypes" => [
                        'id' => "VARCHAR(50)",
                        "name" => "VARCHAR(255) ENCODE LZO"
                    ]
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTable('languages-rs');

        $this->assertEquals("varchar", $table['id']['DATA_TYPE']);
        $this->assertEquals(50, $table['id']['LENGTH']);

        $this->assertEquals("varchar", $table['name']['DATA_TYPE']);
        $this->assertEquals(255, $table['name']['LENGTH']);
    }

    public function testLoadedSortKey() {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);
        
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
                    "destination" => "languages-rs",
                    "sortKey" => "name"
                ]
           ]
        ]);

        $statement = $db->prepare("SELECT \"column\", sortkey FROM pg_table_def WHERE schemaname = ? AND tablename = ? AND \"column\" = ?;");
        $statement->execute([$workspace['connection']['schema'], "languages-rs", "name"]);

        $row = $statement->fetch();

        $this->assertEquals(1,(int)$row['sortkey']);
    }

    /**
     * @dataProvider  distTypeData
     * @param $dist
     */
    public function testLoadedDist($dist) {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN), 'languages-rs',
            new CsvFile($importFile)
        );
        $mapping = [
            "source" => $tableId,
            "destination" => "languages"
        ];
        if (is_array($dist)) {
            $mapping['distKey'] = $dist['key'];
            $mapping['distStyle'] = "key";
        } else {
            $mapping['distStyle'] = $dist;
        }
        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                $mapping
            ]
        ]);

        if (is_array($dist)) {
            $statement = $db->prepare("SELECT \"column\", distkey FROM pg_table_def WHERE schemaname = ? AND tablename = ? AND \"column\" = ?;");
            $statement->execute([$workspace['connection']['schema'], "languages", "id"]);
            $row = $statement->fetch();
            $this->assertEquals(1,(int)$row['distkey']);
        }

        $statement = $db->prepare("select relname, reldiststyle from pg_class where relname = 'languages';");
        $statement->execute();
        $row = $statement->fetch();
        if (is_array($dist)) {
            $this->assertEquals(1, (int) $row['reldiststyle'], "key diststyle doesn't check out.");
        } else if ($dist === 'even') {
            $this->assertEquals(0, (int) $row['reldiststyle'], "even diststyle doesn't check out.");
        } else if ($dist === "all") {
            $this->assertEquals(8, (int) $row['reldiststyle'], "all diststyle doesn't check out.");
        }
    }

    public function distTypeData() {
        return [
            ["all"],
            ["even"],
            [["key" => "id"]]
        ];
    }

}