<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\ExportParamsTest as CommonExportParamsTest;

class ExportParamsTest extends CommonExportParamsTest
{
    public function testTableExportParams()
    {
        $this->markTestSkipped('Table preview with changedSince for Synapse backend is not supported yet');
    }
}
