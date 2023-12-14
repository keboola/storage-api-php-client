<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

use Keboola\Test\StorageApiTestCase;

/**
 * Convert types in input mappings for synapse
 * Actually this class now exists only because
 * synapse don't know integer,character types
 */
final class LegacyInputMappingConverter
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

        if (array_key_exists('datatypes', $input['input'])) {
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
        if (!array_key_exists('datatypes', $input)) {
            return $input;
        }

        $convert = static function ($column, $backendType) {
            if (array_key_exists('type', $column)) {
                $column['type'] = InputMappingConverter::convertColumnOrType($column['type'], $backendType);
            } else {
                foreach ($column as $id => $type) {
                    if (is_array($type)) {
                        $column[$id]['type'] = InputMappingConverter::convertColumnOrType($type['type'], $backendType);
                    } else {
                        $column[$id] = InputMappingConverter::convertColumnOrType($type, $backendType);
                    }
                }
            }
            return $column;
        };

        if (!empty($input['datatypes'])) {
            // columns are in tests also invalid with assoc arr
            $isIndexed = array_values($input['datatypes']) === $input['datatypes'];
            if ($isIndexed === true) {
                $input['datatypes'] = array_map(
                    function ($column) use ($convert, $backendType) {
                        return $convert($column, $backendType);
                    },
                    $input['datatypes'],
                );
            } else {
                $input['datatypes'] = $convert($input['datatypes'], $backendType);
            }
        }
        return $input;
    }
}
