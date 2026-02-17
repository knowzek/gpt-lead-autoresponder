from pydantic import BaseModel, Field, EmailStr
from typing import Optional, Literal

class Conversation(BaseModel):
    """Type class for Conversations table"""
    conversation_id: str = Field(..., description="Unique conversation_id. Format [conv_subscription_id_opp_id")
    opportunity_id: Optional[str] = Field(default=None, description="Opportunity id received from the inbound payload.")
    subscription_id: Optional[str] = Field(default=None, description="Subscription id received from the inbound payload.")
    rooftop_name: Optional[str] = Field(default=None, description="Rooftop name")
    lead_source: Optional[str] = Field(default=None, description="Source / Provider from where the lead has been received [recordId].")
    customer_full_name: Optional[str] = Field(
        default=None, description="Full name of the customer received from the inbound payload."
    )
    customer_email: Optional[EmailStr] = Field(default=None, description="Email of the customer extracted from the inbound payload.")
    customer_phone: Optional[str] = Field(default=None, description="Customer phone number extracted from the inbound payload.")
    salesperson_assigned: Optional[str] = Field(default=None, description="Salesperson which is assigned to handle the customer.")
    last_channel: Optional[Literal['sms', 'email']] = Field(default=None, description="The last channel which was used to communicate with the customer.")
    last_activity_at: Optional[str] = Field(default=None, description="Last activity timestamp datetime -> string (strftime).")
    last_customer_message: Optional[str] = Field(default=None, description="Last message received by the customer.")
    status: Optional[Literal['open', 'closed', 'suppressed', 'needs_review']] = Field(default=None, description="Current status of the lead")
    opted_out: Optional[bool] = Field(default=None)
    opt_out_channel: Optional[Literal['sms', 'email']] = Field(default=None)
    opt_out_at: Optional[str] = Field(default=None)
    linked_lead_record: Optional[str] = Field(
        default=None,
        description="Requires a record id which will be used to link this cell with an existing lead record",
    )
    needs_human_review: Optional[bool] = Field(default=None, description="When human intervention is required or not.")
    needs_human_review_reason: Optional[str] = Field(default=None, description="Reason for the human review.")
    ai_last_reply_at: Optional[str] = Field(default=None, description="Last AI reply timestamp datetime -> string (strftime).")
    customer_last_reply_at: Optional[str] = Field(default=None, description="Last reply timestamp by the customer datetime -> string (strftime).")
    message_count_total: Optional[int] = None
    message_count_inbound: Optional[int] = None
    message_count_outbound: Optional[int] = None


class Message(BaseModel):
    """Type class for Messages table"""
    message_id: str = Field(..., description="Unique id for message received or sent")
    conversation: str = Field(..., description="Reference to the conversation to which this message corresponds (recordId).")
    direction: Literal['inbound', 'outbound'] = Field(..., description="Direction of the message.")
    channel: Literal['sms', 'email'] = Field(..., description="Channel through which the message came in / was sent.")
    timestamp: str = Field(..., description="Timestamp of the message sent/received.")
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = Field(default=None)
    subject: Optional[str] = Field(default=None, description="Only in case when the channel is email.")
    body_text: Optional[str] = Field(default=None, description="Text body of the message sent or received (clean html tags in case of email).")
    body_html: Optional[str] = Field(default=None, description="Email body html (only valid for the case of email).")
    provider: Optional[str] = Field(default=None, description="Providers of the lead.")
    opp_id: str = Field(..., description="Opportunity id received from the inbound payload, indexed for debugging.")
    delivery_status: Literal['sent', 'received', 'failed', 'unknown'] = Field(...)
    rooftop_name: Optional[str] = None
    rooftop_sender: Optional[str] = None
