<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;

class ImportExportCommonTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testRedshiftErrorInCsv(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        try {
            $this->_client->writeTableAsync($tableId, new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages.invalid-data.csv'));
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('invalidData', $e->getStringCode());
        }
    }

    /**
     * Enclosure and escaped by should not be specified together
     */
    public function testRedshiftUnsupportedCsvParams(): void
    {
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $csv = new CsvFile(__DIR__ . '/../../_data/languages.csv', ',', '"', '\\');
        try {
            $this->_client->writeTableAsync($tableId, $csv);
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('csvImport.invalidCsvParams', $e->getStringCode());
        }

        try {
            $this->_client->createTableAsync(
                $this->getTestBucketId(self::STAGE_IN),
                'languages-2',
                $csv,
            );
            $this->fail('Should have thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('csvImport.invalidCsvParams', $e->getStringCode());
        }
    }
}
