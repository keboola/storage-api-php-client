<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 27/07/2016
 * Time: 13:22
 */

namespace Keboola\Test\Backend\Workspaces;


interface WorkspaceBackend
{
    public function getTables();

    public function getTableColumns($table);
    
    public function dropTable($table);
}