<?php

namespace Keboola\StorageApi\GCSUploader;

use Google\Cloud\Core\Exception\ServiceException;

class PromiseHandler
{
    /**
     * @param $results
     * @return array
     */
    public static function getRejected($results)
    {
        $rejected = [];
        foreach ($results as $filePath => $uploadInfo) {
            if ($uploadInfo['state'] === 'rejected') {
                if ($uploadInfo['reason'] instanceof ServiceException) {
                    /** @var ServiceException $reason */
                    $rejected[$filePath] = $uploadInfo['reason'];
                } else {
                    throw new \UnexpectedValueException('Not an instance of S3MultipartUploadException');
                }
            }
        }
        return $rejected;
    }
}
