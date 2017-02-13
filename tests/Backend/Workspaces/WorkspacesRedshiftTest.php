<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesRedshiftTest extends WorkspacesTestCase
{


    public function testCreateNotSupportedBackend()
    {
        $workspaces = new Workspaces($this->_client);
        try {
            $workspaces->createWorkspace(["backend" => "snowflake"]);
            $this->fail("should not be able to create WS for unsupported backend");
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), "workspace.backendNotSupported");
        }
    }

    public function testColumnCompression()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

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
                    "destination" => "languages-rs",
                    "datatypes" => [
                        'id' => "VARCHAR(50)",
                        "name" => "VARCHAR(255) ENCODE BYTEDICT"
                    ]
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTableColumns('languages-rs');

        $this->assertEquals("varchar", $table['id']['DATA_TYPE']);
        $this->assertEquals(50, $table['id']['LENGTH']);

        $this->assertEquals("varchar", $table['name']['DATA_TYPE']);
        $this->assertEquals(255, $table['name']['LENGTH']);

        $stmt = $db->prepare("SELECT * FROM PG_TABLE_DEF WHERE tablename = ?;");
        $stmt->execute(array('languages-rs'));
        $info = $stmt->fetchAll();

        foreach ($info as $colinfo) {
            switch ($colinfo['column']) {
                case "id":
                    $this->assertEquals('lzo', $colinfo['encoding']);
                    break;
                case "name":
                    $this->assertEquals('bytedict', $colinfo['encoding']);
                    break;
            }
        }
    }

    public function testLoadedSortKey()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

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
                    "destination" => "languages-rs",
                    "sortKey" => "name"
                ]
           ]
        ]);

        $statement = $db->prepare("SELECT \"column\", sortkey FROM pg_table_def WHERE schemaname = ? AND tablename = ? AND \"column\" = ?;");
        $statement->execute([$workspace['connection']['schema'], "languages-rs", "name"]);

        $row = $statement->fetch();

        $this->assertEquals(1, (int)$row['sortkey']);
    }

    /**
     * @dataProvider  distTypeData
     * @param $dist
     */
    public function testLoadedDist($dist)
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $db = $this->getDbConnection($workspace['connection']);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
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
            $this->assertEquals(1, (int)$row['distkey']);
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

        foreach ($cols as $colname => $coldata) {
            if (in_array($colname, array_map("strtolower", $primaries))) {
                $this->assertTrue($coldata['PRIMARY']);
                $this->assertEquals("255", $coldata['LENGTH']);
            }
        }

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            "source" => $pkTableId,
            "destination" => "languages-pk-skipped",
            "columns" => ['Paid_Search_Engine_Account','Date'] // missing PK columns
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping2]]);

        $cols = $backend->describeTableColumns("languages-pk-skipped");

        foreach ($cols as $colname => $coldata) {
            if (in_array($colname, array_map("strtolower", $primaries))) {
                $this->assertFalse($coldata['PRIMARY']); // should not set as PK column
                $this->assertEquals("255", $coldata['LENGTH']); // will still be of PK column length
            }
        }
    }

    public function distTypeData()
    {
        return [
            ["all"],
            ["even"],
            [["key" => "id"]]
        ];
    }
}
