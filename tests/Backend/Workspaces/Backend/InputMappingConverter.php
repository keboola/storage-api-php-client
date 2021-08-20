<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Keboola\Test\StorageApiTestCase;

/**
 * Convert types in input mappings for synapse
 * Actually this class now exists only because
 * synapse don't know integer,character types
 */
final class InputMappingConverter
{
    /**
     * @param string $backendType
     * @param array $input
     * @return array
     */
    public static function convertInputColumnsTypesForBackend($backendType, $input)
    {
        if (!in_array($backendType, [
            StorageApiTestCase::BACKEND_SYNAPSE,
            StorageApiTestCase::BACKEND_EXASOL,
        ], true)) {
            return $input;
        }
        if (empty($input['input'])) {
            return $input;
        }

        if (array_key_exists('columns', $input['input'])) {
            $input['input'] = self::convertColumnsDefinition($input['input'], $backendType);
        } else {
            $input['input'] = array_map(static function ($input) use ($backendType) {
                return self::convertColumnsDefinition($input, $backendType);
            }, $input['input']);
        }

        return $input;
    }

    private static function convertColumnsDefinition(array $input, $backendType)
    {
        if (!array_key_exists('columns', $input)) {
            return $input;
        }

        $convert = static function ($column, $backendType) {
            return self::convertColumn($column, $backendType);
        };

        if (!empty($input['columns'])) {
            // columns are in tests also invalid with assoc arr
            $isIndexed = array_values($input['columns']) === $input['columns'];
            if ($isIndexed === true) {
                $input['columns'] = array_map(
                    function ($column) use ($convert, $backendType) {
                        return $convert($column, $backendType);
                    },
                    $input['columns']
                );
            } else {
                $input['columns'] = $convert($input['columns'], $backendType);
            }
        }
        return $input;
    }

    public static function convertColumn($column, $backendType)
    {
        if ($backendType === StorageApiTestCase::BACKEND_SYNAPSE) {
            switch (strtolower($column['type'])) {
                case 'integer':
                    $column['type'] = 'int';
                    break;
                case 'character':
                    $column['type'] = 'char';
                    break;
            }
        }

        if ($backendType === StorageApiTestCase::BACKEND_EXASOL) {
            switch (strtolower($column['type'])) {
                case 'integer':
                    $column['type'] = 'DECIMAL';
                    $column['length'] = '3,0';
                    break;
                case 'varchar':
                    if (!array_key_exists('length', $column)) {
                        $column['length'] = '2000000';
                    }
                    break;
                case 'character':
                    $column['type'] = 'char';
                    if (!array_key_exists('length', $column)) {
                        $column['length'] = '2000';
                    }
                    break;
            }
        }

        return $column;
    }
}
