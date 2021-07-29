<?php

namespace Keboola\Test\Backend\Exasol;

use Keboola\Test\Backend\CommonPart1\BucketsTest as CommonBuckets;

class BucketsTest extends CommonBuckets
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testBucketManipulation()
    {
        $this->markTestSkipped();
    }
}
