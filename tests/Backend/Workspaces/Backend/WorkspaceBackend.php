<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 27/07/2016
 * Time: 13:22
 */

namespace Keboola\Test\Backend\Workspaces\Backend;

interface WorkspaceBackend
{
    public function getTables();

    public function createTable($tableName, $columns);
    
    public function getTableColumns($table);
    
    public function dropTable($table);

    public function dropTableColumn($table, $column);

    public function countRows($table);

    public function fetchAll($table);

    // This will return the identifier as it would be returned by the backend.
    public function toIdentifier($item);

    public function describeTableColumns($tableName);
}
