<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Mixed\SnowflakeRedshift\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Mixed\LegacyWorkspacesBaseCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class LegacyWorkspacesTest extends LegacyWorkspacesBaseCase
{

    /**
     * @dataProvider loadToRedshiftDataTypes
     * @param $dataTypesDefinition
     */
    public function testDataTypesLoadToRedshift($dataTypesDefinition)
    {

        $bucketBackend = self::BACKEND_SNOWFLAKE;

        if ($this->_client->bucketExists("out.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket(
                "out.c-mixed-test-{$bucketBackend}",
                [
                    'force' => true,
                ]
            );
        }

        if ($this->_client->bucketExists("in.c-mixed-test-" . $bucketBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$bucketBackend}", [
                'force' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$bucketBackend}", "in", "", $bucketBackend);

        //setup test table
        $this->_client->createTable(
            $bucketId,
            'dates',
            new CsvFile(__DIR__ . '/../../../../_data/dates.csv')
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace([
            'backend' => self::BACKEND_REDSHIFT,
        ]);

        $options = [
            "input" => [
                [
                    "source" => "in.c-mixed-test-{$bucketBackend}.dates",
                    "destination" => "dates",
                    "datatypes" => $dataTypesDefinition
                ]
            ]
        ];

        // exception should not be thrown, date conversion should be applied
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $data = $wsBackend->fetchAll("dates", \PDO::FETCH_ASSOC);
        $this->assertCount(3, $data);
    }

    public function loadToRedshiftDataTypes()
    {
        return [
            [['valid_from' => "TIMESTAMP"]],
            [[['column' => 'valid_from', 'type' => "TIMESTAMP"]]]
        ];
    }

    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
        ];
    }

    public function workspaceMixedAndSameBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT],
        ];
    }

    public function workspaceMixedAndSameBackendDataWithDataTypes()
    {
        $simpleDataTypesDefinitionSnowflake = ["price" => "VARCHAR", "quantity" => "NUMBER"];
        $simpleDataTypesDefinitionRedshift = ["price" => "VARCHAR", "quantity" => "INTEGER"];
        $extendedDataTypesDefinitionSnowflake = [["column" => "price", "type" => "VARCHAR"], ["column" => "quantity", "type" => "NUMBER"]];
        $extendedDataTypesDefinitionRedshift = [["column" => "price", "type" => "VARCHAR"], ["column" => "quantity", "type" => "INTEGER"]];
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionSnowflake],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionRedshift],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT, $simpleDataTypesDefinitionRedshift],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $extendedDataTypesDefinitionSnowflake],
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT, $extendedDataTypesDefinitionSnowflake],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE, $extendedDataTypesDefinitionRedshift],
            [self::BACKEND_REDSHIFT, self::BACKEND_REDSHIFT, $extendedDataTypesDefinitionRedshift],

        ];
    }
}
