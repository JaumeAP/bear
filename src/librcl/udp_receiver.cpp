/* Copyright Institute of Sound and Vibration Research - All rights reserved */

#include "udp_receiver.hpp"

#include <libpml/empty_parameter_config.hpp>

#include <boost/array.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/bind/bind.hpp>
#ifndef VISR_DISABLE_THREADS
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/thread.hpp>
#endif


#include <ciso646>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace visr
{
namespace rcl
{

class UdpReceiver::Impl
{
public:
    Impl( std::size_t port,
          UdpReceiver::Mode mode);

    ~Impl();
    void process(UdpReceiver::MessageOutput & messageOutput);

private:

    void handleReceiveData(const boost::system::error_code& error,
        std::size_t numBytesTransferred);

    Mode const mMode;

    /**
    * Pointer to the either internally or externally provided externally provided boost::asio::io_context object.
    */
    boost::asio::io_context* mIoContext;

    /**
    * An actual io_context object owned by this component, which is allocated in the modes Synchronous or Asynchronous,
    * but not for ExternalServiceObject.
    */
    std::unique_ptr<boost::asio::io_context> mIoContextInstance;

    std::unique_ptr<boost::asio::ip::udp::socket> mSocket;

    boost::asio::ip::udp::endpoint mRemoteEndpoint;

    boost::array<char, cMaxMessageLength> mReceiveBuffer;

    using IoContextWorkGuard =
        boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::unique_ptr<IoContextWorkGuard> mIoContextWork;

    /**
    * Internal queue of messages received asynchronously. They will be copied into the output
    *  MessageQueue in the process() function. An object is instantiated only in the asynchronous mode.
    */
    std::deque< pml::StringParameter > mInternalMessageBuffer;

#ifndef VISR_DISABLE_THREADS
    std::unique_ptr< boost::thread > mServiceThread;

    boost::mutex mMutex;
#endif
};

UdpReceiver::UdpReceiver( SignalFlowContext const & context,
                          char const * name,
                          CompositeComponent * parent,
                          std::size_t port,
                          Mode mode)
 : AtomicComponent(context, name, parent)
 , mImpl( new Impl( port, mode ) )
 , mDatagramOutput("messageOutput", *this, pml::EmptyParameterConfig())
{
}

UdpReceiver::~UdpReceiver() = default;

void UdpReceiver::process()
{
  mImpl->process(mDatagramOutput);
}

// ==========================================================================
// Implementation class

UdpReceiver::Impl::Impl( std::size_t port,
                         Mode mode )
 : mMode( mode )
{
    using boost::asio::ip::udp;
    mIoContextInstance.reset(new boost::asio::io_context());
    mIoContext = mIoContextInstance.get();

    if (mMode == Mode::Synchronous)
    {
        mIoContextWork.reset();
    }
    else
    {
        mIoContextWork.reset(new IoContextWorkGuard(mIoContext->get_executor()));
    }
    mSocket.reset(new udp::socket(*mIoContext));
    boost::system::error_code ec;
    mSocket->open(udp::v4(), ec);
    mSocket->set_option(boost::asio::socket_base::reuse_address(true));
    mSocket->bind(udp::endpoint(udp::v4(), static_cast<unsigned short>(port)));

    if (ec)
    {
        throw std::runtime_error("Error opening UDP port");
    }

    mSocket->async_receive_from(boost::asio::buffer(mReceiveBuffer),
        mRemoteEndpoint,
        boost::bind(&UdpReceiver::Impl::handleReceiveData, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred)
    );
#ifdef VISR_DISABLE_THREADS
    throw std::invalid_argument( "UdpReceiver: Asynchronous mode is not supported because threads are disabled." );
#else
    if (mMode == Mode::Asynchronous)
    {
        mServiceThread.reset(new boost::thread(boost::bind(&boost::asio::io_context::run, mIoContext)));
    }
#endif // VISR_DISABLE_THREADS
}

UdpReceiver::Impl::~Impl()
{
  if( mIoContextInstance.get() != nullptr )
  {
    mIoContextInstance->stop();
  }
#ifndef VISR_DISABLE_THREADS
  if( mServiceThread.get() != nullptr  )
  {
    mServiceThread->join();
  }
#endif
}

void UdpReceiver::Impl::process( UdpReceiver::MessageOutput & messageOutput )
{
  if(  mMode == Mode::Synchronous )
  {
    mIoContext->poll();
  }
#ifndef VISR_DISABLE_THREADS
  boost::lock_guard<boost::mutex> lock( mMutex );
#endif
  while( not mInternalMessageBuffer.empty() )
  {
    pml::StringParameter const & nextMsg = mInternalMessageBuffer.front();
    messageOutput.enqueue( nextMsg  );
    mInternalMessageBuffer.pop_front();
  }
}

void UdpReceiver::Impl::handleReceiveData( const boost::system::error_code& error,
                                           std::size_t numBytesTransferred )
{
  {
#ifndef VISR_DISABLE_THREADS
    boost::lock_guard<boost::mutex> lock( mMutex );
#endif
    mInternalMessageBuffer.push_back( pml::StringParameter( std::string( &mReceiveBuffer[0], numBytesTransferred ) ) );
  }
  mSocket->async_receive_from( boost::asio::buffer(mReceiveBuffer),
                               mRemoteEndpoint,
                               boost::bind(&Impl::handleReceiveData, this,
                                            boost::asio::placeholders::error,
                                            boost::asio::placeholders::bytes_transferred)
                             );
}

} // namespace rcl
} // namespace visr
