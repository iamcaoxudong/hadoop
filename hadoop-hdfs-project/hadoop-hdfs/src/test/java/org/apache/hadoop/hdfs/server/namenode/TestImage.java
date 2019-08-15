package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URL;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.HttpServerFunctionalTest;
import org.junit.Test;
import org.mockito.Mockito;

public class TestImage {

  /**
   * Test to verify the timeout of Image upload
   */
  @Test
  public void testImage() throws Exception {
    byte[] buffer = DFSUtil.string2Bytes(RandomStringUtils.randomAscii(1024*1024*10));

    Configuration conf = new HdfsConfiguration();
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    HttpServer2 testServer = HttpServerFunctionalTest.createServer("hdfs");
    try {
      testServer.addServlet("ImageTransfer", ImageServlet.PATH_SPEC,
          TestImageTransferServlet.class);
      testServer.start();
      URL serverURL = HttpServerFunctionalTest.getServerURL(testServer);
      // set the timeout here, otherwise it will take default.
      TransferFsImage.timeout = 2000;

      File tmpDir = new File(new FileSystemTestHelper().getTestRootDir());
      tmpDir.mkdirs();

      File mockImageFile = File.createTempFile("image", "", tmpDir);
      FileOutputStream imageFile = new FileOutputStream(mockImageFile);
      imageFile.write(buffer);
      imageFile.close();
      Mockito.when(
          mockStorage.findImageFile(Mockito.any(NameNodeFile.class),
              Mockito.anyLong())).thenReturn(mockImageFile);
      Mockito.when(mockStorage.toColonSeparatedString()).thenReturn(
          "storage:info:string");
      
      try {
        TransferFsImage.uploadImageFromStorage(serverURL, conf, mockStorage,
            NameNodeFile.IMAGE, 1L);
        fail("TransferImage Should fail with timeout");
      } catch (SocketTimeoutException e) {
        assertEquals("Upload should timeout", "Read timed out", e.getMessage());
      }
    } finally {
      testServer.stop();
    }
  }

  public static class TestImageTransferServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      synchronized (this) {
        try {
          wait(5000);
        } catch (InterruptedException e) {
          // Ignore
        }
      }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      synchronized (this) {
        ServletContext context = getServletContext();
        final FSImage nnImage = NameNodeHttpServer.getFsImageFromContext(context);
        if (nnImage != null) {
          resp.sendError(HttpServletResponse.SC_EXPECTATION_FAILED,
              "Nameode is currently not in a state which can "
                  + "accept uploads of new fsimages.");
        }
      }
    }
  }
}
