<?php

namespace Keboola\StorageApi;

/**
 * @param int $maxJobPollWaitSeconds
 * @return \callable
 */
function createSimpleJobPollDelay($maxJobPollWaitSeconds = 20)
{
    return function ($tries) use ($maxJobPollWaitSeconds) {
        return min(pow(2, $tries), $maxJobPollWaitSeconds);
    };
}
