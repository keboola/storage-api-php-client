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

    public function dropTableIfExists($table);

    public function dropTableColumn($table, $column);

    public function countRows($table);

    /**
     * @param string $table
     * @param int $style
     * @param string|null $orderBy
     */
    public function fetchAll($table, $style = \PDO::FETCH_NUM, $orderBy = null);

    // This will return the identifier as it would be returned by the backend.
    public function toIdentifier($item);

    public function describeTableColumns($tableName);
}
