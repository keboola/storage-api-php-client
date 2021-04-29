<?php

namespace Keboola\Test\Backend;

trait WorkspaceCredentialsAssertTrait
{
    /**
     * @param array $connection
     * @throws \Exception
     */
    private function assertCredentialsShouldNotWork($connection)
    {
        try {
            $this->getDbConnection($connection);
            $this->fail('Credentials should be invalid');
        } catch (\Doctrine\DBAL\Driver\PDOException $e) {
            // Synapse
            if (!in_array(
                $e->getCode(),
                [
                    //https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
                    '28000', // Invalid authorization specification
                    '08004', // Server rejected the connection
                ],
                true
            )) {
                $this->fail(sprintf('Unexpected error code "%s" for Synapse credentials fail.', $e->getCode()));
            }
        } catch (\PDOException $e) {
            // RS
            $this->assertEquals(7, $e->getCode());
        } catch (\Keboola\Db\Import\Exception $e) {
            $this->assertContains('Incorrect username or password was specified', $e->getMessage());
        }
    }
}
