<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 29/10/14
 * Time: 14:10
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options\BucketCredentials;

class ListCredentialsOptions
{

    private $bucketId;

    /**
     * @return mixed
     */
    public function getBucketId()
    {
        return $this->bucketId;
    }

    /**
     * @param $bucketId
     * @return $this
     */
    public function setBucketId($bucketId)
    {
        $this->bucketId = $bucketId;
        return $this;
    }
}
