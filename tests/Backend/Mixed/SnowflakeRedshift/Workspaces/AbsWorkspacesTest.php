<?php

namespace Keboola\Test\Backend\Mixed\SnowflakeRedshift\Workspaces;

use Keboola\Test\Backend\Mixed\BaseWorkSpacesTestCase;

class AbsWorkspacesTest extends BaseWorkSpacesTestCase
{
    public function workspaceBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, ["amount" => "NUMBER"]],
        ];
    }

    public function workspaceMixedAndSameBackendDataWithDataTypes()
    {
        $simpleDataTypesDefinitionSnowflake = [
            [
                'source' => 'price',
                'type' => 'VARCHAR',
            ],
            [
                'source' => 'quantity',
                'type' =>'NUMBER',
            ],
        ];

        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE, $simpleDataTypesDefinitionSnowflake],
        ];
    }

    public function workspaceMixedAndSameBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_SNOWFLAKE],
        ];
    }
}
