<?php
namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\DataPreviewLimitsTest as CommonDataPreviewLimitsTest;

class DataPreviewLimitsTest extends CommonDataPreviewLimitsTest
{
    public function testJsonTruncationLimit()
    {
        $this->markTestSkipped('Columns with large length for Synapse backend is not supported yet');
    }
}
