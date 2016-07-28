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

    public function getTableColumns($table);
    
    public function dropTable($table);

    // This will return the item as it would be returned by the backend.
    // Required because bloody redshift does -- Standard and delimited identifiers are case-insensitive and are folded to lower case
    public function toIdentifier($item);
}