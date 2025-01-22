<?php

declare(strict_types=1);

namespace Backend\Snowflake;

use Generator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class WorkspacesLoginNameTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    /**
     * @return Generator<string, array{async:bool}>
     */
    public static function syncAsyncProvider(): Generator
    {
        yield 'sync' => [
            'async' => false,
        ];
        yield 'async' => [
            'async' => true,
        ];
    }

    /**
     * @dataProvider syncAsyncProvider
     */
    public function testWorkspaceCreate(bool $async): void
    {
        $this->allowTestForBackendsOnly(
            [self::BACKEND_SNOWFLAKE],
            'Test only for Snowflake login types',
        );
        $this->initEvents($this->workspaceSapiClient);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspace = $this->initTestWorkspace('snowflake', [], true, $async);

        // test connection is working
        $db = $this->getDbConnectionSnowflakeDBAL($workspace['connection']);

        /** @var string $currentUser */
        $currentUser = $db->fetchOne('SELECT CURRENT_USER()');
        /** @var array<int, array{property: string, value: string}> $res */
        $res = $db->fetchAllAssociative(sprintf('DESC USER %s', SnowflakeQuote::quoteSingleIdentifier($currentUser)));
        /** @var array{int:array{property: 'LOGIN_NAME', value: string}} $loginName */
        $loginName = array_filter($res, fn($row) => $row['property'] === 'LOGIN_NAME');
        $loginName = reset($loginName);

        // login name by default same as name of the user
        $this->assertSame($currentUser, $loginName['value']);
    }
}
