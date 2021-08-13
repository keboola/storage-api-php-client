<?php

namespace Keboola\Test\Backend\Exasol;

class CreateTableTest extends \Keboola\Test\Backend\CommonPart1\CreateTableTest
{
    public function testTableCreateWithPK()
    {
        $this->markTestSkipped('Needs createTablePrimaryKey');
    }

    public function testRowNumberAmbiguity()
    {
        $this->markTestSkipped('Needs createTablePrimaryKey');
    }
}
