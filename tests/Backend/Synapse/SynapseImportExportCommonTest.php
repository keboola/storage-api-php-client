<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\ImportExportCommonTest;
use Keboola\Csv\CsvFile;

class SynapseImportExportCommonTest extends ImportExportCommonTest
{
    /**
     * @return array<string, array<mixed>>
     */
    public function incrementalImportPkDedupeData(): array
    {
        return [
            'simple' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
                    'id',
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.loaded.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.loaded.csv'),
                ],
            'multiple' =>
                [
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.csv'),
                    'id,sub_id',
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.loaded.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple.increment.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple_synapse.increment.loaded.csv'),
                ],
        ];
    }
}
