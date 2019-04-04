<?php

declare(strict_types=1);

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class TriggersTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testCreateTrigger()
    {
        $table1 = $this->createTableWithRandomData("watched-1");
        $table2 = $this->createTableWithRandomData("watched-2");
        $newTokenId = $this->_client->createToken([$this->getTestBucketId() => 'read']);

        $trigger = $this->_client->createTrigger([
            'componentType' => 'orchestrator',
            'configurationId' => 123,
            'coolDownPeriod' => 10,
            'runWithTokenId' => $newTokenId,
            'tableIds' => [
                $table1,
                $table2
            ]
        ]);
    }
}
