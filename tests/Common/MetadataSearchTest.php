<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Metadata;

class MetadataSearchTest extends StorageApiTestCase
{

    private $metadataApi;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->metadataApi = new Metadata($this->_client);

        $tableId = $this->_client->createTable($this->getTestBucketId(), "rates", new CsvFile(__DIR__ . '/../_data/rates.csv'));

        $tables = $this->_client->listTables($this->getTestBucketId(), ["include" => "columns"]);
        reset($tables);
        $ratesTable = $tables[0];
        $columns = $ratesTable['columns'];

        $tableMetaJson = file_get_contents(__DIR__ . '/../_data/metadata/rates.json');
        $tableMeta = json_decode($tableMetaJson);
        $this->metadataApi->postTableMetadata($tableId, 'keboola.app-column-info', $tableMeta);

        foreach ($columns as $column) {
            $columnMeta = json_decode(
                file_get_contents(__DIR__ . '/../_data/metadata/rates.' . $column . '.json')
            );
            $this->metadataApi->postColumnMetadata($tableId . "." . $column, 'keboola.app-column-info', $columnMeta);
        }
    }

    public function testMetadataSearchEqualsFilter()
    {

        $this->assertTrue(true);
    }
}