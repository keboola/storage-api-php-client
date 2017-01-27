<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class DeleteTableTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testTableDelete()
    {
        $table1Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $table2Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages_2', new CsvFile(__DIR__ . '/../../_data/languages.csv'));
        $tables = $this->_client->listTables($this->getTestBucketId(self::STAGE_IN));

        $this->assertCount(2, $tables);
        $this->_client->dropTable($table1Id);

        $tables = $this->_client->listTables($this->getTestBucketId(self::STAGE_IN));
        $this->assertCount(1, $tables);

        $table = reset($tables);
        $this->assertEquals($table2Id, $table['id']);
    }
}
