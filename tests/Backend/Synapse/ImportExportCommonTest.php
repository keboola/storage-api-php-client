<?php



namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\ImportExportCommonTest as CommonImportExportTest;

class ImportExportCommonTest extends CommonImportExportTest
{
    public function testTableAsyncExportRepeatedly()
    {
        $this->markTestSkipped('Exporting table table for Synapse backend is not supported yet');
    }
}
