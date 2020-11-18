<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 */

namespace Keboola\Test\Backend\Mixed\SnowflakeRedshift\Workspaces;

use Keboola\Test\Backend\Mixed\WorkspacesRenameBaseCase;

class WorkspacesRenameTest extends WorkspacesRenameBaseCase
{
    public function workspaceMixedBackendData()
    {
        return [
            [self::BACKEND_SNOWFLAKE, self::BACKEND_REDSHIFT],
            [self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE],
        ];
    }
}
