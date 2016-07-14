/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Creates a new Alluxio file system, generating empty files in the root directory with random name.
 */
public class MasterLoadGenerator {
  private static class RandomString {

    private static final char[] SYMBOLS;

    private final Random mRandom = new Random();

    private final char[] mBuf;

    static {
      StringBuilder tmp = new StringBuilder();
      for (char ch = '0'; ch <= '9';++ch) {
        tmp.append(ch);
      }
      for (char ch = 'a'; ch <= 'z';++ch) {
        tmp.append(ch);
      }
      SYMBOLS = tmp.toString().toCharArray();
    }

    RandomString(int length) {
      if (length < 1) {
        throw new IllegalArgumentException("length < 1: " + length);
      }
      mBuf = new char[length];
    }

    String nextString() {
      mBuf[0] = '/';
      for (int idx = 1; idx < mBuf.length;++idx) {
        mBuf[idx] = SYMBOLS[mRandom.nextInt(SYMBOLS.length)];
      }
      return new String(mBuf);
    }
  }

  private static final String MNT_PATH = "/mnt";
  private static final int FILE_NUM = 100;
  private static final int DIR_NUM = 100;
  private static final int LIST_TIMES = 100000;
  private static final int INTERVAL = 1000;
  private static final int THREAD_NUMBER = 8;


  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(Constants.GB, Constants.GB,
          Constants.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(Constants.KB));

  private AlluxioMaster mAlluxioMaster;
  private FileSystemMaster mFileSystemMaster;

  @Before
  public final void before() throws Exception {
    mAlluxioMaster = mLocalAlluxioClusterResource.get().getMaster().getInternalMaster();
    mFileSystemMaster = mAlluxioMaster.getFileSystemMaster();
  }

  public class ListStatThread implements Runnable {
    private final int mListTimes;
    private final int mId;

    public ListStatThread(int listTimes, int id) {
      mListTimes = listTimes;
      mId = id;
    }

    @Override
    public void run() {
      try {
        listStatus(mListTimes, mId);
      } catch (InvalidPathException | FileDoesNotExistException | AccessControlException e) {
        e.printStackTrace();
      }
    }
  }

  public void directoryGenerate()
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException, BlockInfoException {
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt"), CreateDirectoryOptions.defaults());
    for(int i = 0; i < DIR_NUM; i++) {
      mFileSystemMaster.createDirectory(dirURL(i), CreateDirectoryOptions.defaults());
      for(int j = 0; j < FILE_NUM; j++) {
        mFileSystemMaster.createFile(fileURL(i, j), CreateFileOptions.defaults());
      }
    }
  }

  public void listStatus(int listTimes, int id)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException {
    long startTime = System.nanoTime();
    for (int k = 1; k <= listTimes; k++) {
      if (k % INTERVAL == 0) {
        long duration = System.nanoTime() - startTime;
        System.out.printf("Thread_%d, k:%d,  Throughput: %d /ms,  timeduration: %d ns%n ",  id, k,
            (INTERVAL*1000000) /duration, duration);

      }
      startTime = System.nanoTime();
      int i = ThreadLocalRandom.current().nextInt(0, DIR_NUM);
      mFileSystemMaster.getFileInfoList(dirURL(i), ListStatusOptions.defaults().isLoadDirectChildren());
    }
  }

  private AlluxioURI dirURL(int i) {
    AlluxioURI path = new AlluxioURI(MNT_PATH + "/D" + Integer.toString(i));
    return path;
  }

  private AlluxioURI fileURL(int i, int j) {

    AlluxioURI path = new AlluxioURI(MNT_PATH + "/D" + Integer.toString(i)
        + "/F" + Integer.toString(j));
    return path;
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      Constants.SECURITY_AUTHENTICATION_TYPE, "NOSASL",
      Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false",
      Constants.SECURITY_GROUP_MAPPING, JournalIntegrationTest.FakeUserGroupsMapping.FULL_CLASS_NAME})
  public void listTest()
      throws AccessControlException, IOException, FileDoesNotExistException, InvalidPathException,
      FileAlreadyExistsException, BlockInfoException {
    String user = "alluxio";
    Configuration.set(Constants.SECURITY_LOGIN_USERNAME, user);
    directoryGenerate();
    for (int i = 0; i < THREAD_NUMBER; i++) {
      ListStatThread listStatThread =
          new ListStatThread(LIST_TIMES / THREAD_NUMBER, i);
      new Thread(listStatThread).start();
    }
  }
}
