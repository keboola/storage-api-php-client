<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

use \Keboola\StorageApi\Options\FileUploadOptions,
	\Keboola\StorageApi\Options\ListFilesOptions;

class Keboola_StorageApi_FilesTest extends StorageApiTestCase
{



	public function testFileList()
	{
		$options = new FileUploadOptions();
		$fileId = $this->_client->uploadFile(__DIR__ . '/_data/files.upload.txt', $options);
		$files = $this->_client->listFiles(new ListFilesOptions());
		$this->assertNotEmpty($files);
		$this->assertEquals($fileId, reset($files)['id']);
	}

	public function testFilesListFilterByTags()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';

		$this->_client->uploadFile($filePath, new FileUploadOptions());
		$tag = uniqid('tag-test');
		$fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setTags(array($tag)));

		$files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag)));

		$this->assertCount(1, $files);
		$file = reset($files);
		$this->assertEquals($fileId, $file['id']);
	}

	public function testFileListSearch()
	{

		$fileId = $this->_client->uploadFile(__DIR__ . '/_data/users.csv', new FileUploadOptions());
		$this->_client->uploadFile(__DIR__ . '/_data/files.upload.txt', new FileUploadOptions());

		$files = $this->_client->listFiles((new ListFilesOptions())->setQuery('users')->setLimit(1));

		$this->assertCount(1, $files);
		$file = reset($files);
		$this->assertEquals($fileId, $file['id']);
	}

	/**
	 * @dataProvider uploadData
	 */
	public function testFileUpload($filePath, FileUploadOptions $options)
	{
		$fileId = $this->_client->uploadFile($filePath, $options);
		$file = $this->_client->getFile($fileId);

		$this->assertEquals($options->getIsPublic(), $file['isPublic']);
		$this->assertEquals(basename($filePath), $file['name']);
		$this->assertEquals(filesize($filePath), $file['sizeBytes']);
		$this->assertEquals(file_get_contents($filePath), file_get_contents($file['url']));

		$tags = $options->getTags();
		sort($tags);
		$fileTags = $file['tags'];
		sort($fileTags);
		$this->assertEquals($tags, $fileTags);

		$info = $this->_client->getLogData();
		$this->assertEquals($file['creatorToken']['id'], (int) $info['id']);
		$this->assertEquals($file['creatorToken']['description'], $info['description']);

		if ($options->getIsPermanent()) {
			$this->assertNull($file['maxAgeDays']);
		} else {
			$this->assertInternalType('integer', $file['maxAgeDays']);
			$this->assertEquals(180, $file['maxAgeDays']);
		}
	}

	public function testFileUploadUsingFederationToken()
	{
		$pathToFile = __DIR__ . '/_data/files.upload.txt';
		$options = new FileUploadOptions();
		$options
			->setFileName('upload.txt')
			->setFederationToken(true);

		$result = $this->_client->prepareFileUpload($options);

		$uploadParams = $result['uploadParams'];
		$this->assertArrayHasKey('credentials', $uploadParams);

		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'],
			'Body'   => fopen($pathToFile, 'r+'),
		));

		$file = $this->_client->getFile($result['id']);

		$this->assertEquals(file_get_contents($pathToFile), file_get_contents($file['url']));

		try {
			$s3Client->putObject(array(
				'Bucket' => $uploadParams['bucket'],
				'Key'    => $uploadParams['key'] . '_part0001',
				'Body'   => fopen($pathToFile, 'r+'),
			));
			$this->fail('Access denied exception should be thrown');
		} catch (\Aws\S3\Exception\AccessDeniedException $e) {}
	}

	public function testSlicedFileUpload()
	{
		$pathToFile = __DIR__ . '/_data/files.upload.txt';
		$options = new FileUploadOptions();
		$options
			->setIsSliced(true)
			->setFileName('upload.txt');

		$preparedFile = $this->_client->prepareFileUpload($options);

		$uploadParams = $preparedFile['uploadParams'];
		$this->assertArrayHasKey('credentials', $uploadParams);
		$this->assertTrue($preparedFile['isSliced']);

		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001',
			'Body'   => fopen($pathToFile, 'r+'),
		))->get('ObjectURL');
		$part2URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part002',
			'Body'   => fopen($pathToFile, 'r+'),
		))->get('ObjectURL');

		$manifest = array(
			'entries' => array(
				array(
					'url' => $part1URL,
				),
				array(
					'url' => $part2URL,
				)
			),
		);
		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode($manifest),
		));

		$file = $this->_client->getFile($preparedFile['id']);
		$this->assertEquals(json_encode($manifest), file_get_contents($file['url']));

		// download sliced file
		$file = $this->_client->getFile($preparedFile['id'], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));
		$this->assertTrue($file['isSliced']);

		$downloadCredentials = new Aws\Common\Credentials\Credentials(
			$file['credentials']['AccessKeyId'],
			$file['credentials']['SecretAccessKey'],
			$file['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $downloadCredentials));

		$objects = $s3Client->listObjects(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Prefix' => $file['s3Path']['key'],
		));

		$this->assertCount(3, $objects->get('Contents'));

		$object = $s3Client->getObject(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Key' => $file['s3Path']['key'] . 'manifest',
		));
		$this->assertEquals(json_encode($manifest), $object['Body']);

		$object = $s3Client->getObject(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Key' => $file['s3Path']['key'] . 'part001',
		));
		$this->assertEquals(file_get_contents($pathToFile), $object['Body']);

		$object = $s3Client->getObject(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Key' => $file['s3Path']['key'] . 'part002',
		));
		$this->assertEquals(file_get_contents($pathToFile), $object['Body']);
	}


	/**
	 * @dataProvider uploadData with compress = true
	 */
	public function testFileUploadCompress()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';
		$fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setCompress(true));
		$file = $this->_client->getFile($fileId);

		$this->assertEquals(basename($filePath) . ".gz", $file['name']);

		$gzFile = gzopen($file['url'], "r");
		$this->assertEquals(file_get_contents($filePath), gzread($gzFile, 524288));
	}

	public function uploadData()
	{
		$path  = __DIR__ . '/_data/files.upload.txt';;
		return array(
			array(
				$path,
				(new FileUploadOptions())
			),
			array(
				$path,
				(new FileUploadOptions())
					->setNotify(false)
					->setCompress(false)
					->setIsPublic(false)
			),
			array(
				$path,
				(new FileUploadOptions())
					->setIsPublic(true)
					->setIsPermanent(true)
					->setTags(array('sapi-import', 'martin'))
			),
		);
	}



	public function testFilesPermissions()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';
		$uploadOptions = new FileUploadOptions();

		$newTokenId = $this->_client->createToken(array(), 'Files test');
		$newToken = $this->_client->getToken($newTokenId);
		$firstFileId = $this->_client->uploadFile($filePath, $uploadOptions);

		$totalFilesCount = count($this->_client->listFiles());
		$this->assertNotEmpty($totalFilesCount);

		// new token should not have access to any files
		$newTokenClient = new Keboola\StorageApi\Client($newToken['token'], STORAGE_API_URL);
		$this->assertEmpty($newTokenClient->listFiles());

		$newFileId = $newTokenClient->uploadFile($filePath, $uploadOptions);
		$files = $newTokenClient->listFiles();
		$this->assertCount(1, $files);
		$this->assertEquals($newFileId, reset($files)['id']);

		// new file should be visible for master token
		$files = $this->_client->listFiles();
		$this->assertEquals($newFileId, reset($files)['id']);

		$this->_client->dropToken($newTokenId);

		// new token wil all bucket permissions
		$newTokenId = $this->_client->createToken(array(), 'files manage', null, true);
		$newToken = $this->_client->getToken($newTokenId);


		$this->_client->dropToken($newTokenId);
	}

	public function testGetFileFederationToken()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';
		$fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setNotify(false)->setIsPublic(false));

		$file = $this->_client->getFile($fileId, (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

		$this->assertArrayHasKey('credentials', $file);
		$this->assertArrayHasKey('s3Path', $file);
		$this->assertArrayHasKey('Expiration', $file['credentials']);

		$credentials = new Aws\Common\Credentials\Credentials(
			$file['credentials']['AccessKeyId'],
			$file['credentials']['SecretAccessKey'],
			$file['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$object = $s3Client->getObject(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Key' => $file['s3Path']['key'],
		));
		$this->assertEquals(file_get_contents($filePath), $object['Body']);

		/**
		 * @var \Guzzle\Service\Resource\Model $objects
		 */
		$objects = $s3Client->listObjects(array(
			'Bucket' => $file['s3Path']['bucket'],
			'Prefix' => $file['s3Path']['key'],
		));

		$this->assertCount(1, $objects->get('Contents'), 'Only one file should be returned');

		try {
			$s3Client->listObjects(array(
				'Bucket' => $file['s3Path']['bucket'],
				'Prefix' => dirname($file['s3Path']['key']),
			));
			$this->fail('Access denied exception should be thrown');
		} catch (\Aws\S3\Exception\AccessDeniedException $e) {}

		try {
			$s3Client->listObjects(array(
				'Bucket' => $file['s3Path']['bucket'],
				'Prefix' => $file['s3Path']['key'] . 'manifest',
			));
			$this->fail('Access denied exception should be thrown');
		} catch (\Aws\S3\Exception\AccessDeniedException $e) {}

	}

	public function testTagging()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';
		$initialTags = array('gooddata', 'image');
		$fileId = $this->_client->uploadFile($filePath, (new FileUploadOptions())->setTags($initialTags));

		$file = $this->_client->getFile($fileId);
		$this->assertEquals($initialTags, $file['tags']);

		$this->_client->deleteFileTag($fileId, 'gooddata');

		$file = $this->_client->getFile($fileId);
		$this->assertEquals(array('image'), $file['tags']);

		$this->_client->addFileTag($fileId, 'new');
		$file = $this->_client->getFile($fileId);
		$this->assertEquals(array('image', 'new'), $file['tags']);
	}

}
