<?php

namespace Keboola\Test\Backend\Exasol;

class ExportParamsTest extends \Keboola\Test\Backend\CommonPart1\ExportParamsTest
{
    public function testTableExportParams()
    {
        $this->markTestSkipped('Needs incremental load');
    }
}
