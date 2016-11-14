<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\Mysql;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class ImportExportTest extends StorageApiTestCase
{

    /**
     * @var \Keboola\StorageApi\BucketCredentials
     */
    private $bucketCredentials;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();

        $this->bucketCredentials = new \Keboola\StorageApi\BucketCredentials($this->_client);
    }

    public function testImportWithWarnings()
    {
        $importFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);

        $warningsFile = new CsvFile(__DIR__ . '/../../_data/warnings.languages.csv');
        $result = $this->_client->writeTable($tableId, $warningsFile);

        $this->assertCount(2, reset($result['warnings']));

        $this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../../_data/warnings.languages-export.csv'), $this->_client->exportTable($tableId), 'imported data comparsion');
    }

    public function testTableImportInvalidLineBreaks()
    {
        $importCsvFile = new CsvFile(__DIR__ . '/../../_data/escaping.mac-os-9.csv');
        try {
            $this->_client->createTable($this->getTestBucketId(), 'languages', $importCsvFile);
            $this->fail('Mac os 9 line breaks should not be allowd');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
        }

        try {
            $this->_client->createTableAsync($this->getTestBucketId(), 'languages', $importCsvFile);
            $this->fail('Mac os 9 line breaks should not be allowd');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
        }

        $createCsvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $createCsvFile);
        try {
            $this->_client->writeTable($tableId, $importCsvFile);
            $this->fail('Mac os 9 line breaks should not be allowd');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
        }
    }
}
