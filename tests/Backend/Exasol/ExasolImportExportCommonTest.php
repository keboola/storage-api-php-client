<?php

namespace Keboola\Test\Backend\Exasol;

use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\CommonPart1\ImportExportCommonTest;
use Keboola\Csv\CsvFile;

class ExasolImportExportCommonTest extends ImportExportCommonTest
{
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
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple_exasol.csv'),
                    'id,sub_id',
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple_exasol.loaded.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple_exasol.increment.csv'),
                    new CsvFile(__DIR__ . '/../../_data/pk.multiple_exasol.increment.loaded.csv'),
                ],
        ];
    }

    public function testTableImportNullValuesOnPrimaryKey(): void
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.null.csv');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Load error: Constraint violation - not null (column name).');
        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-null',
            $importFile,
            [
                'primaryKey' => 'name',
            ],
        );
    }
}
