<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\ExportParamsTest as CommonExportParamsTest;

class ExportParamsTest extends CommonExportParamsTest
{
    public function testTableExportParams()
    {
        $this->markTestSkipped('Table preview with changedSince for Synapse backend is not supported yet');
    }

    public function testTableExportAsyncCache()
    {
        $this->markTestSkipped('Exporting table table for Synapse backend is not supported yet');
    }

    /**
     * Test access to cached file by various tokens
     */
    public function testTableExportAsyncPermissions()
    {
        $this->markTestSkipped('Exporting table table for Synapse backend is not supported yet');
    }
}
