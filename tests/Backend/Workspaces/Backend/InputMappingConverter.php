<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

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
        if ($backendType !== 'synapse') {
            return $input;
        }
        if (empty($input['input'])) {
            return $input;
        }

        if (array_key_exists('columns', $input['input'])) {
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
        if (!array_key_exists('columns', $input)) {
            return $input;
        }

        $convert = static function ($column) {
            switch (strtolower($column['type'])) {
                case 'integer':
                    $column['type'] = 'int';
                    break;
                case 'character':
                    $column['type'] = 'char';
                    break;
            }
            return $column;
        };

        if (!empty($input['columns'])) {
            // columns are in tests also invalid with assoc arr
            $isIndexed = array_values($input['columns']) === $input['columns'];
            if ($isIndexed === true) {
                $input['columns'] = array_map($convert, $input['columns']);
            } else {
                $input['columns'] = $convert($input['columns']);
            }
        }
        return $input;
    }
}
