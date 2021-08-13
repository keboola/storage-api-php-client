<?php

namespace Keboola\Test\Backend\Exasol;

class SimpleAliasTest extends \Keboola\Test\Backend\CommonPart2\SimpleAliasTest
{
    public function testTableAlias()
    {
        $this->markTestSkipped('missing incremental load');
    }

    public function testSourceTableColumnDeleteWithAutoSyncAliases()
    {
        $this->markTestSkipped('missing removeTableColumn');
    }

    public function testSourceTableColumnAddWithAutoSyncAliases()
    {
        $this->markTestSkipped('missing addTableColumn');
    }

    public function testAliasColumnsAutoSync()
    {
        $this->markTestSkipped('missing addTableColumn');
    }

    public function testColumnAssignedToAliasWithAutoSyncShouldNotBeDeletable()
    {
        $this->markTestSkipped('missing addTableColumn');
    }

    public function testColumnUsedInFilteredAliasShouldNotBeDeletable()
    {
        $this->markTestSkipped('missing deleteTableColumn');
    }

    public function testColumnUsedInFilteredAliasShouldNotBeForceDeletable()
    {
        $this->markTestSkipped('missing deleteTableColumn');
    }

    public function testColumnAssignedToAliasWithoutAutoSyncShouldNotBeDeletable()
    {
        $this->markTestSkipped('missing deleteTableColumn');
    }

    public function testColumnAssignedToAliasWithoutAutoSyncShouldNotBeForceDeletable()
    {
        $this->markTestSkipped('missing deleteTableColumn');
    }

    public function testColumnNotUsedInAnyAliasShouldBeDeletable()
    {
        $this->markTestSkipped('missing deleteTableColumn');
    }

    public function testAliasColumns()
    {
        $this->markTestSkipped('missing addTableColumn');
    }
}
