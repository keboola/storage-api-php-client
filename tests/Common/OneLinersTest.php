<?php
/**
 *
 * User: Ondřej Hlaváček
 * Date: 7.8.12
 * Time: 16:40
 *
 */
namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class OneLinersTest extends StorageApiTestCase
{

    protected $_inBucketId;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testOneLinerCreate()
    {
        \Keboola\StorageApi\OneLiner::setClient($this->_client);
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $oneLiner = $this->uploadOneLiner($testBucketId);
        $this->assertEquals(1, $oneLiner->id);
        $this->assertEquals("whatever" . PHP_EOL . "new line", $oneLiner->name);
    }

    public function testOneLinerLoad()
    {
        \Keboola\StorageApi\OneLiner::setClient($this->_client);
        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $oneLiner = $this->uploadOneLiner($testBucketId);
        $oneLiner->save();

        $oneLiner2 = new \Keboola\StorageApi\OneLiner($testBucketId . ".oneLinerTest");

        $this->assertEquals(1, $oneLiner2->id);
        $this->assertEquals("whatever" . PHP_EOL . "new line", $oneLiner2->name);
    }

    private function uploadOneLiner($buckeId)
    {
        $oneLiner = new \Keboola\StorageApi\OneLiner($buckeId . ".oneLinerTest");
        $oneLiner->id = 1;
        $oneLiner->name = "whatever" . PHP_EOL . "new line";
        return $oneLiner;
    }
}
