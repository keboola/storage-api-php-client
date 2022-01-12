<?php

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;

class TempDevBranchOnlyTest extends StorageApiTestCase
{
    /**
     * @return void
     */
    public function testProjectHasFeatureAndEnvVariableDefined()
    {
        $token = $this->_client->verifyToken();

        if (in_array(self::FEATURE_CONFIGURATIONS_USE_DEV_BRANCH_SERVICES_ONLY, $token['owner']['features']) && $this->shouldUseLegacyBranchServices() === false) {
            $this->assertTrue(true);
        } else {
            $this->fail(sprintf(
                "Project has not feautre '%s' or test has not ENV variable '%s' defined.",
                self::FEATURE_CONFIGURATIONS_USE_DEV_BRANCH_SERVICES_ONLY,
                self::ENV_USE_DEV_BRANCH_SERVICES_ONLY
            ));
        }
    }
}
