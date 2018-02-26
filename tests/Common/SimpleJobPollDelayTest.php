<?php


namespace Keboola\Test\Common;

use function Keboola\StorageApi\createSimpleJobPollDelay;
use Keboola\Test\StorageApiTestCase;

class SimpleJobPollDelayTest extends StorageApiTestCase
{

    public function testSimpleJobPollDelayParam()
    {
        $retryDelay = createSimpleJobPollDelay(10);

        $this->assertEquals(1, $retryDelay(0));
        $this->assertEquals(2, $retryDelay(1));
        $this->assertEquals(4, $retryDelay(2));
        $this->assertEquals(8, $retryDelay(3));
        $this->assertEquals(10, $retryDelay(4));
        $this->assertEquals(10, $retryDelay(5));
        $this->assertEquals(10, $retryDelay(5000));
    }

    public function testSimpleJobPollDelayDefault()
    {
        $retryDelay = createSimpleJobPollDelay();

        $this->assertEquals(1, $retryDelay(0));
        $this->assertEquals(2, $retryDelay(1));
        $this->assertEquals(4, $retryDelay(2));
        $this->assertEquals(8, $retryDelay(3));
        $this->assertEquals(16, $retryDelay(4));
        $this->assertEquals(20, $retryDelay(5));
        $this->assertEquals(20, $retryDelay(5000));
    }
}
