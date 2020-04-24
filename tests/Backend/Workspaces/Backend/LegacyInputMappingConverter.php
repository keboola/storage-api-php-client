<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

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
        if ($backendType !== 'synapse') {
            return $input;
        }
        if (empty($input['input'])) {
            return $input;
        }

        if (array_key_exists('datatypes', $input['input'])) {
            $input['input'] = self::convertColumnsDefinition($input['input']);
        } else {
            $input['input'] = array_map(static function ($input) {
                return self::convertColumnsDefinition($input);
            }, $input['input']);
        }

        return $input;
    }

    private static function convertColumnsDefinition(array $input)
    {
        if (!array_key_exists('datatypes', $input)) {
            return $input;
        }

        $convert = static function ($column) {
            if (array_key_exists('type', $column)) {
                $column['type'] = InputMappingConverter::convertType($column['type']);
            } else {
                foreach ($column as $id => $type) {
                    if (is_array($type)) {
                        $column[$id]['type'] = InputMappingConverter::convertType($type['type']);
                    } else {
                        $column[$id] = InputMappingConverter::convertType($type);
                    }
                }
            }
            return $column;
        };

        if (!empty($input['datatypes'])) {
            // columns are in tests also invalid with assoc arr
            $isIndexed = array_values($input['datatypes']) === $input['datatypes'];
            if ($isIndexed === true) {
                $input['datatypes'] = array_map($convert, $input['datatypes']);
            } else {
                $input['datatypes'] = $convert($input['datatypes']);
            }
        }
        return $input;
    }
}
