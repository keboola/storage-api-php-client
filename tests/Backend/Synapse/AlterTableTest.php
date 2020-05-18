<?php

namespace Keboola\Test\Backend\Synapse;

use Keboola\Test\Backend\CommonPart1\AlterTableTest as CommonAlterTableTest;

class AlterTableTest extends CommonAlterTableTest
{

    public function testPrimaryKeyAddRequiredParam()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyAddWithSameColumnsInDifferentBuckets()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testPrimaryKeyAddWithDuplicty()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }

    public function testAddInvalidPrimaryKey()
    {
        $this->markTestSkipped('Modifying PK for Synapse backend is not supported yet');
    }
}
