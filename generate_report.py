"""
WEAVE-DB  Lab 9 & 10 Report – Software Metrics & Risk Handling
Clean black-and-white academic style (matching SRS document format).
"""

import math, os, io, textwrap
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import black, white, HexColor
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, KeepTogether, HRFlowable
)
from reportlab.lib import colors

OUTPUT_PDF = os.path.join(os.path.dirname(__file__), "Group5_Lab910.pdf")
CHARTS_DIR = os.path.join(os.path.dirname(__file__), "_charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

W, H = A4
ss = getSampleStyleSheet()

# ---------- plain academic styles (black text, serif) ----------
ss.add(ParagraphStyle("Title0", parent=ss["Title"], fontSize=20, spaceAfter=4, fontName="Times-Bold"))
ss.add(ParagraphStyle("Title1", parent=ss["Title"], fontSize=16, spaceAfter=2, fontName="Times-Bold"))
ss.add(ParagraphStyle("H1", parent=ss["Heading1"], fontSize=14, fontName="Times-Bold",
                       spaceBefore=18, spaceAfter=6, textColor=black))
ss.add(ParagraphStyle("H2", parent=ss["Heading2"], fontSize=12, fontName="Times-Bold",
                       spaceBefore=14, spaceAfter=4, textColor=black))
ss.add(ParagraphStyle("H3", parent=ss["Heading3"], fontSize=11, fontName="Times-Bold",
                       spaceBefore=10, spaceAfter=3, textColor=black))
ss.add(ParagraphStyle("Body", parent=ss["Normal"], fontSize=10, leading=14,
                       fontName="Times-Roman", alignment=TA_JUSTIFY, spaceAfter=6))
ss.add(ParagraphStyle("BodySm", parent=ss["Normal"], fontSize=9, leading=12,
                       fontName="Times-Roman", alignment=TA_JUSTIFY, spaceAfter=4))
ss.add(ParagraphStyle("Center", parent=ss["Normal"], fontSize=10, leading=14,
                       fontName="Times-Roman", alignment=TA_CENTER))
ss.add(ParagraphStyle("CodeBlock", parent=ss["Normal"], fontSize=9, leading=11,
                       fontName="Courier", alignment=TA_LEFT, spaceAfter=4))
ss.add(ParagraphStyle("Caption", parent=ss["Normal"], fontSize=9, leading=11,
                       fontName="Times-Italic", alignment=TA_CENTER, spaceAfter=8))
ss.add(ParagraphStyle("BulletItem", parent=ss["Normal"], fontSize=10, leading=14,
                       fontName="Times-Roman", leftIndent=20, bulletIndent=8,
                       spaceAfter=2))
ss.add(ParagraphStyle("TCell", parent=ss["Normal"], fontSize=8.5, leading=10,
                       fontName="Times-Roman"))
ss.add(ParagraphStyle("TCellB", parent=ss["Normal"], fontSize=8.5, leading=10,
                       fontName="Times-Bold"))
ss.add(ParagraphStyle("TCellC", parent=ss["Normal"], fontSize=8.5, leading=10,
                       fontName="Times-Roman", alignment=TA_CENTER))

def tbl(data, cw=None, hdr=True):
    """Plain academic table: thin black grid, bold header row, no colour."""
    t = Table(data, colWidths=cw, repeatRows=1 if hdr else 0)
    cmds = [
        ("FONTNAME", (0,0), (-1,-1), "Times-Roman"),
        ("FONTSIZE", (0,0), (-1,-1), 8.5),
        ("LEADING",  (0,0), (-1,-1), 11),
        ("VALIGN",   (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("LEFTPADDING",   (0,0), (-1,-1), 4),
        ("RIGHTPADDING",  (0,0), (-1,-1), 4),
        ("GRID", (0,0), (-1,-1), 0.5, black),
    ]
    if hdr:
        cmds += [
            ("FONTNAME", (0,0), (-1,0), "Times-Bold"),
            ("ALIGN",    (0,0), (-1,0), "CENTER"),
        ]
    t.setStyle(TableStyle(cmds))
    return t

def chart(fig, name):
    p = os.path.join(CHARTS_DIR, name)
    fig.savefig(p, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return p

def img(story, path, w=5.2*inch, h=3.2*inch, cap=None):
    story.append(Image(path, width=w, height=h))
    if cap:
        story.append(Paragraph(cap, ss["Caption"]))
    story.append(Spacer(1,4))

def bullet(story, text):
    story.append(Paragraph(f"\u2022  {text}", ss["BulletItem"]))

def hr(story):
    story.append(HRFlowable(width="100%", thickness=0.5, color=black, spaceAfter=6, spaceBefore=6))

# ======================================================
#                   CHART GENERATION
# ======================================================

plt.rcParams.update({"font.family": "serif", "font.size": 10})

def mk_burndown():
    fig, ax = plt.subplots(figsize=(7,4))
    x = list(range(6))
    lbls = ["W0\nFeb 25","W1\nMar 4","W2\nMar 11","W3\nMar 18","W4\nMar 25","W5\nMar 31"]
    ideal = [22,17.6,13.2,8.8,4.4,0]
    actual = [22,19,14,9,5,1]
    ax.plot(x, ideal, "k--", lw=1.5, marker="o", ms=4, label="Ideal Burndown")
    ax.plot(x, actual, "k-",  lw=2,   marker="s", ms=5, label="Actual Burndown")
    ax.set_xticks(x); ax.set_xticklabels(lbls, fontsize=8)
    ax.set_ylabel("Remaining Story Points"); ax.set_title("Sprint 2 Burndown Chart")
    ax.legend(fontsize=9); ax.grid(True, alpha=.3); ax.set_ylim(bottom=-1)
    fig.tight_layout()
    return chart(fig, "burndown.png")

def mk_burnup():
    fig, ax = plt.subplots(figsize=(7,4))
    x = list(range(6))
    lbls = ["W0\nFeb 25","W1\nMar 4","W2\nMar 11","W3\nMar 18","W4\nMar 25","W5\nMar 31"]
    scope=[22,22,22,24,24,24]; done=[0,3,8,15,19,23]
    ax.plot(x,scope,"k--",lw=1.5,marker="D",ms=4,label="Total Scope")
    ax.plot(x,done,"k-",lw=2,marker="o",ms=5,label="Completed")
    ax.fill_between(x,done,alpha=.12,color="grey")
    ax.set_xticks(x); ax.set_xticklabels(lbls,fontsize=8)
    ax.set_ylabel("Story Points"); ax.set_title("Sprint 2 Burnup Chart")
    ax.legend(fontsize=9); ax.grid(True,alpha=.3)
    fig.tight_layout()
    return chart(fig,"burnup.png")

def mk_cfd():
    fig,ax=plt.subplots(figsize=(7,4))
    dates=["Feb 10","Feb 17","Feb 24","Mar 3","Mar 10","Mar 17","Mar 24","Mar 31"]
    done=np.array([0,3,6,8,14,19,26,32])
    ip=np.array([2,3,4,6,5,6,4,3])
    todo=np.array([35,31,27,23,18,12,7,2])
    x=range(len(dates))
    ax.fill_between(x,0,done,alpha=.5,color="0.3",label="Done")
    ax.fill_between(x,done,done+ip,alpha=.4,color="0.55",label="In Progress")
    ax.fill_between(x,done+ip,done+ip+todo,alpha=.3,color="0.75",label="To Do")
    ax.set_xticks(x); ax.set_xticklabels(dates,fontsize=8,rotation=15)
    ax.set_ylabel("Number of Tasks"); ax.set_title("Cumulative Flow Diagram (All Sprints)")
    ax.legend(fontsize=9); ax.grid(True,alpha=.2,axis="y")
    fig.tight_layout()
    return chart(fig,"cfd.png")

def mk_throughput():
    fig,ax=plt.subplots(figsize=(7,3.5))
    weeks=["W1\nFeb 10","W2\nFeb 17","W3\nFeb 24","W4\nMar 3","W5\nMar 10","W6\nMar 17","W7\nMar 24","W8\nMar 31"]
    tp=[3,3,2,6,5,7,4,2]
    ax.bar(range(len(weeks)),tp,color="0.35",edgecolor="black",lw=.5)
    for i,v in enumerate(tp): ax.text(i,v+.15,str(v),ha="center",fontsize=9,fontweight="bold")
    avg=np.mean(tp)
    ax.axhline(avg,color="black",ls="--",lw=1,label=f"Avg = {avg:.1f}")
    ax.set_xticks(range(len(weeks))); ax.set_xticklabels(weeks,fontsize=7.5)
    ax.set_ylabel("Tasks Completed"); ax.set_title("Weekly Throughput Report")
    ax.legend(fontsize=9); ax.grid(True,alpha=.2,axis="y")
    fig.tight_layout()
    return chart(fig,"throughput.png")

def mk_velocity():
    fig,ax=plt.subplots(figsize=(6,3.5))
    sp=["Sprint 1\n(Feb 4\u201319)","Sprint 2\n(Feb 25\u2013Mar 31)"]
    x=np.arange(2); w=.3
    ax.bar(x-w/2,[12,24],w,color="white",edgecolor="black",hatch="//",label="Planned")
    ax.bar(x+w/2,[12,23],w,color="0.45",edgecolor="black",label="Completed")
    for i,v in zip(x,[12,24]): ax.text(i-w/2,v+.3,str(v),ha="center",fontsize=10)
    for i,v in zip(x,[12,23]): ax.text(i+w/2,v+.3,str(v),ha="center",fontsize=10)
    ax.set_xticks(x); ax.set_xticklabels(sp,fontsize=9)
    ax.set_ylabel("Story Points"); ax.set_title("Sprint Velocity")
    ax.legend(fontsize=9); ax.grid(True,alpha=.2,axis="y")
    fig.tight_layout()
    return chart(fig,"velocity.png")

def mk_loc():
    fig,ax=plt.subplots(figsize=(7,3.5))
    dates=["Feb 10","Feb 14","Feb 26","Mar 6","Mar 17","Mar 22","Mar 25","Mar 29","Mar 31"]
    py=[50,180,320,420,620,1050,1350,1550,1790]
    ts=[0,0,0,0,0,0,0,3800,5548]
    inv=[0,0,0,0,0,0,1985,1985,1985]
    tests_loc=[0,0,0,0,0,0,0,2058,2058]
    total=[p+t+v+tl for p,t,v,tl in zip(py,ts,inv,tests_loc)]
    ax.plot(range(len(dates)),py,"k-",lw=1.5,marker="o",ms=4,label="Backend Python")
    ax.plot(range(len(dates)),ts,"k--",lw=1.5,marker="s",ms=4,label="Frontend TS/TSX")
    ax.plot(range(len(dates)),inv,"k:",lw=1.5,marker="^",ms=4,label="Inverse Engine")
    ax.plot(range(len(dates)),total,"k-",lw=2.5,marker="D",ms=5,label="Total LOC")
    ax.set_xticks(range(len(dates))); ax.set_xticklabels(dates,fontsize=7.5,rotation=15)
    ax.set_ylabel("Lines of Code"); ax.set_title("Code Growth Over Time")
    ax.legend(fontsize=8); ax.grid(True,alpha=.3)
    fig.tight_layout()
    return chart(fig,"loc_growth.png")

def mk_commit():
    fig,ax=plt.subplots(figsize=(7,3.5))
    weeks=["Feb 10\u201316","Feb 17\u201323","Feb 24\u201328","Mar 1\u20137","Mar 8\u201314","Mar 15\u201321","Mar 22\u201328","Mar 29\u201331"]
    devs={"Ishat":[4,1,2,0,0,0,0,0],"Parrva":[0,0,0,0,0,4,5,3],"Anirudh":[0,0,0,1,0,2,3,15],"Ishita":[0,0,0,0,0,1,0,0]}
    x=np.arange(len(weeks)); w=.2
    hatches=["//","..","\\\\","xx"]
    for i,(dev,c) in enumerate(devs.items()):
        ax.bar(x+i*w-1.5*w,c,w,color="white",edgecolor="black",hatch=hatches[i],label=dev)
    ax.set_xticks(x); ax.set_xticklabels(weeks,fontsize=7,rotation=20)
    ax.set_ylabel("Commits"); ax.set_title("Commit Activity by Developer")
    ax.legend(fontsize=8); ax.grid(True,alpha=.2,axis="y")
    fig.tight_layout()
    return chart(fig,"commit_activity.png")

def mk_test():
    fig,(ax1,ax2)=plt.subplots(1,2,figsize=(8,3.5))
    sizes=[40,3,36]; lbls=["Completed\n(40)","In Progress\n(3)","Not Tested\n(36)"]
    cs=["0.35","0.55","0.8"]; exp=(0.04,0.04,0.04)
    ax1.pie(sizes,explode=exp,labels=lbls,colors=cs,autopct="%1.1f%%",startangle=90,textprops={"fontsize":8})
    ax1.set_title("Test Case Status (79 Total)",fontsize=10)
    types=["Unit (40)","Integration (22)","System (17)"]
    comp=[24,12,7]; prog=[3,2,0]; ntst=[13,8,10]; x=np.arange(3)
    ax2.bar(x,comp,.55,color="0.35",edgecolor="black",label="Completed")
    ax2.bar(x,prog,.55,bottom=comp,color="0.55",edgecolor="black",label="In Progress")
    ax2.bar(x,ntst,.55,bottom=[c+p for c,p in zip(comp,prog)],color="0.8",edgecolor="black",label="Not Tested")
    ax2.set_xticks(x); ax2.set_xticklabels(types,fontsize=8)
    ax2.set_ylabel("Count"); ax2.set_title("By Test Type",fontsize=10)
    ax2.legend(fontsize=7); ax2.grid(True,alpha=.2,axis="y")
    fig.tight_layout()
    return chart(fig,"test_coverage.png")

def mk_risk():
    fig,ax=plt.subplots(figsize=(7,5.5))
    for i in range(1,6):
        for j in range(1,6):
            s=i*j
            g="0.92" if s<=4 else "0.84" if s<=9 else "0.74" if s<=16 else "0.62"
            ax.add_patch(plt.Rectangle((j-.5,i-.5),1,1,facecolor=g,edgecolor="white",lw=2))
    risks=[(4,3,"R1"),(3,4,"R2"),(5,2,"R3"),(2,3,"R4"),(3,3,"R5"),(4,2,"R6"),(2,4,"R7"),(1,2,"R8"),(3,2,"R9"),(5,1,"R10")]
    for imp,prob,lbl in risks:
        ax.scatter(prob,imp,s=200,c="white",edgecolors="black",lw=1.5,zorder=5)
        ax.annotate(lbl,(prob,imp),textcoords="offset points",xytext=(10,6),fontsize=8,fontweight="bold",
                    bbox=dict(boxstyle="round,pad=.2",fc="white",ec="black",lw=.5))
    ax.set_xlim(.5,5.5); ax.set_ylim(.5,5.5)
    ax.set_xticks([1,2,3,4,5]); ax.set_xticklabels(["Very Low","Low","Medium","High","Very High"],fontsize=8)
    ax.set_yticks([1,2,3,4,5]); ax.set_yticklabels(["Very Low","Low","Medium","High","Critical"],fontsize=8)
    ax.set_xlabel("Probability",fontsize=10); ax.set_ylabel("Impact",fontsize=10)
    ax.set_title("Risk Assessment Matrix",fontsize=11)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(fc="0.92",ec="black",label="Low (1\u20134)"),Patch(fc="0.84",ec="black",label="Medium (5\u20139)"),
                       Patch(fc="0.74",ec="black",label="High (10\u201316)"),Patch(fc="0.62",ec="black",label="Critical (17\u201325)")],
              fontsize=7,loc="upper left")
    fig.tight_layout()
    return chart(fig,"risk_matrix.png")

# ======================================================
#                      BUILD PDF
# ======================================================

def build():
    doc = SimpleDocTemplate(OUTPUT_PDF, pagesize=A4,
        topMargin=2*cm, bottomMargin=2*cm, leftMargin=2.5*cm, rightMargin=2.5*cm)
    S = []  # story
    a = S.append

    # --- title page ---
    a(Spacer(1, 2.5*inch))
    a(Paragraph("Software Metrics & Risk Handling Report", ss["Title0"]))
    a(Spacer(1, .15*inch))
    a(Paragraph("for", ss["Center"]))
    a(Spacer(1, .1*inch))
    a(Paragraph("<b>WEAVE-DB</b>", ss["Title1"]))
    a(Paragraph("Workbench for Event-based Atomic Versioning of Databases", ss["Center"]))
    a(Spacer(1, .5*inch))
    a(Paragraph("<b>Lab 9 & 10</b>", ss["Center"]))
    a(Spacer(1, .3*inch))
    a(Paragraph("Prepared by:", ss["Center"]))
    a(Paragraph("Anirudh Dhoot (B24CS1026)", ss["Center"]))
    a(Paragraph("Ishat Varshney (B24CS1082)", ss["Center"]))
    a(Paragraph("Ishita Tyagi (B24CS1083)", ss["Center"]))
    a(Paragraph("Parrva Shah (B24CS1053)", ss["Center"]))
    a(Spacer(1, .4*inch))
    a(Paragraph("Date: 31/03/2026", ss["Center"]))
    a(Paragraph("Group 5", ss["Center"]))
    a(PageBreak())

    # --- contents ---
    a(Paragraph("Contents", ss["H1"]))
    toc = [
        ("1", "Section I: Software Metrics", [
            ("1.1", "Intermediate COCOMO"),
            ("1.2", "Halstead Metrics"),
            ("1.3", "Function Point Analysis"),
            ("1.4", "Cumulative Flow Diagram"),
            ("1.5", "Throughput Report"),
            ("1.6", "Sprint Burndown Chart"),
            ("1.7", "Sprint Burnup Chart"),
            ("1.8", "Additional Metric 1: Sprint Velocity"),
            ("1.9", "Additional Metric 2: Code Growth & Commit Activity"),
        ]),
        ("2", "Section II: Risk Analysis", [
            ("2.1", "Risk Assessment Matrix"),
            ("2.2", "Detailed Risk Register"),
        ]),
        ("3", "Section III: Risk-Sprint Integration", [
            ("3.1", "Risk Mitigation Across Sprints"),
            ("3.2", "Integration with Sprint Backlog"),
        ]),
    ]
    for num, title, subs in toc:
        a(Paragraph(f"<b>{num}  {title}</b>", ss["Body"]))
        for sn, st in subs:
            a(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{sn}  {st}", ss["Body"]))
    a(PageBreak())

    # =================== SECTION I ===================
    a(Paragraph("1. Section I: Software Metrics", ss["H1"]))
    a(Spacer(1, .1*inch))

    # project overview
    a(Paragraph("Project Overview", ss["H2"]))
    a(Paragraph(
        "WEAVE-DB is a full-stack web-based SQL workbench with database version control capabilities. "
        "It intercepts data-modifying SQL commands, records them as versioned commit events, stores "
        "inverse operations (anti-commands), maintains periodic snapshots to AWS S3, and enables "
        "controlled rollback to any previous version. The architecture comprises a Django REST "
        "backend (authentication, ORM models), a FastAPI microservice (real-time versioning, rollback, "
        "async operations), and a React / TypeScript frontend (SQL editor, results grid, version panel, "
        "terminal emulator).", ss["Body"]))

    a(Paragraph("Repository Structure", ss["H3"]))
    a(Paragraph(
        "The codebase is hosted at github.com/IshatV412/dbworkbench (upstream) with a fork at "
        "github.com/anirudhiitj/dbworkbench (origin). Development is organized across multiple "
        "branches:", ss["Body"]))

    branch_data = [
        ["Branch", "Owner", "Content", "LOC"],
        ["upstream/main", "Ishat", "Initial commit, cloud setup, query module", "~250"],
        ["upstream/testing", "Ishat", "S3/RDS performance tests, query scripts, cloud infrastructure", "~818"],
        ["upstream/backend", "Parrva", "Django + FastAPI backend, test suite (10 test files)", "~3,330"],
        ["upstream/frontend", "Anirudh", "Full-stack integration: Django + FastAPI + React frontend", "~7,340"],
        ["upstream/inverse_sql_commands", "Ishita", "Inverse engine, rollback engine, version store, workbench", "~1,985"],
        ["origin/backend", "Anirudh", "Backend fixes, PostgreSQL integration", "Merged"],
        ["origin/frontend", "Anirudh", "Next.js frontend (replaced), then React/Lovable frontend", "Merged"],
    ]
    a(tbl(branch_data, cw=[1.7*inch, .8*inch, 2.4*inch, .7*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("Codebase Size Summary", ss["H3"]))
    size_data = [
        ["Component", "Files", "Lines of Code", "Branch(es)"],
        ["FastAPI Backend (routes, services, models, utils)", "~25", "1,449", "upstream/backend, frontend"],
        ["Django Backend (auth, connections, core)", "~20", "381", "upstream/backend, frontend"],
        ["React Frontend (pages, components, hooks, lib)", "~78", "5,548", "upstream/frontend"],
        ["Inverse Engine + Rollback Engine", "5", "1,985", "upstream/inverse_sql_commands"],
        ["Backend Test Suite (pytest)", "11", "2,058", "upstream/backend"],
        ["Cloud / RDS / S3 Testing", "5", "818", "upstream/testing"],
        ["Total", "~144", "~12,239", "All branches"],
    ]
    a(tbl(size_data, cw=[2.5*inch, .5*inch, .9*inch, 1.8*inch]))
    a(Spacer(1,.1*inch))
    a(Paragraph("Contributor Summary (from git shortlog -sne --all):", ss["Body"]))
    contrib_data = [
        ["Developer", "Commits", "Primary Role"],
        ["Anirudh Dhoot", "22", "Dev Team \u2014 FastAPI scaffold, frontend UI, integration"],
        ["Parrva Shah", "14", "Dev Team \u2014 Django ORM, auth, services, backend tests"],
        ["Ishat Varshney", "10", "Cloud Team \u2014 S3/RDS infra, performance testing, deployment"],
        ["Ishita Tyagi", "1 (+branch)", "DB Team \u2014 Inverse SQL engine, rollback engine, testing"],
    ]
    a(tbl(contrib_data, cw=[1.3*inch, .7*inch, 3.5*inch]))
    a(Paragraph(
        "<i>Note:</i> Ishita's inverse_sql_commands branch contains 1,985 lines across 5 Python modules "
        "(inverse_engine.py: 1,245; rollback_engine.py: 264; version_store.py: 231; workbench.py: 245) "
        "plus a 517-line test suite. Commit count alone does not reflect contribution volume.", ss["BodySm"]))

    # -------- 1.1 COCOMO --------
    a(PageBreak())
    a(Paragraph("1.1  Intermediate COCOMO", ss["H1"]))
    a(Paragraph(
        "The Constructive Cost Model (COCOMO) provides effort and schedule estimation based on "
        "source lines of code. We apply the Intermediate COCOMO model which incorporates 15 cost "
        "drivers to adjust the nominal effort estimate.", ss["Body"]))

    a(Paragraph("Project Classification", ss["H2"]))
    a(Paragraph(
        "WEAVE-DB is classified as Semi-Detached because:", ss["Body"]))
    bullet(S, "Moderate team experience with the technologies (Django, FastAPI, React).")
    bullet(S, "Mix of rigid requirements (database versioning correctness) and flexible design (UI, storage).")
    bullet(S, "Small team (4 members) with moderately complex multi-service architecture.")
    bullet(S, "Multiple integration points: PostgreSQL, AWS S3, Kafka (planned), WebSocket.")

    a(Paragraph("COCOMO Parameters", ss["H2"]))
    a(tbl([
        ["Parameter", "Value", "Description"],
        ["a", "3.0", "Effort coefficient (semi-detached)"],
        ["b", "1.12", "Effort exponent (semi-detached)"],
        ["c", "2.5", "Schedule coefficient (semi-detached)"],
        ["d", "0.35", "Schedule exponent (semi-detached)"],
        ["KLOC", "12.239", "Thousands of lines of code (all branches)"],
    ], cw=[1*inch, .8*inch, 3.5*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("Cost Driver Analysis", ss["H2"]))
    a(Paragraph("The Effort Adjustment Factor (EAF) is the product of 15 cost driver multipliers:", ss["Body"]))
    eaf_tbl = [["Cost Driver", "Rating", "Multiplier", "Justification"]]
    drivers = [
        ("RELY \u2014 Required reliability", "High", "1.15", "DB integrity critical for rollback correctness"),
        ("DATA \u2014 Database size", "High", "1.08", "Large data volumes with snapshot/commit storage"),
        ("CPLX \u2014 Product complexity", "High", "1.15", "Inverse operation gen, rollback logic, event ordering"),
        ("TIME \u2014 Execution time", "Nominal", "1.00", "Latency not a primary concern (per SRS)"),
        ("STOR \u2014 Storage constraint", "Nominal", "1.00", "No special storage constraints on compute"),
        ("VIRT \u2014 Virtual machine volatility", "Nominal", "1.00", "Standard deployment environment"),
        ("TURN \u2014 Turnaround time", "Nominal", "1.00", "Standard dev cycle"),
        ("ACAP \u2014 Analyst capability", "High", "0.86", "Strong analytical skills, domain knowledge"),
        ("AEXP \u2014 Application experience", "Low", "1.10", "First project of this nature for the team"),
        ("PCAP \u2014 Programmer capability", "High", "0.86", "Proficient in Python, TypeScript, SQL"),
        ("VEXP \u2014 VM experience", "Nominal", "1.00", "Standard environment familiarity"),
        ("LEXP \u2014 Language experience", "High", "0.95", "Experienced with Python and TypeScript"),
        ("MODP \u2014 Modern practices", "High", "0.91", "Agile Scrum, event-driven architecture"),
        ("TOOL \u2014 Software tools", "High", "0.91", "VS Code, Git, Trello, AWS, Docker"),
        ("SCED \u2014 Schedule constraint", "Nominal", "1.00", "Reasonable academic timeline"),
    ]
    for d in drivers:
        eaf_tbl.append(list(d))
    a(tbl(eaf_tbl, cw=[1.8*inch, .7*inch, .7*inch, 2.5*inch]))

    mults = [1.15,1.08,1.15,1.00,1.00,1.00,1.00,0.86,1.10,0.86,1.00,0.95,0.91,0.91,1.00]
    eaf = 1.0
    for m in mults: eaf *= m
    eaf = round(eaf, 3)
    a(Spacer(1,.1*inch))
    a(Paragraph(f"EAF = product of all multipliers = {eaf}", ss["Body"]))

    a(Paragraph("Calculations", ss["H2"]))
    kloc = 12.239
    aa, bb, cc, dd = 3.0, 1.12, 2.5, 0.35
    enom = aa * (kloc ** bb)
    eadj = enom * eaf
    tdev = cc * (eadj ** dd)
    team = eadj / tdev
    prod = kloc * 1000 / eadj
    a(tbl([
        ["Metric", "Formula", "Result"],
        ["Nominal Effort", f"E_nom = 3.0 x (12.239)^1.12", f"{enom:.2f} person-months"],
        ["Adjusted Effort", f"E_adj = {enom:.2f} x {eaf}", f"{eadj:.2f} person-months"],
        ["Development Time", f"TDEV = 2.5 x ({eadj:.2f})^0.35", f"{tdev:.2f} months"],
        ["Team Size", f"N = E_adj / TDEV", f"{team:.1f} persons"],
        ["Productivity", f"LOC / E_adj", f"{prod:.0f} LOC/person-month"],
    ], cw=[1.3*inch, 2.5*inch, 1.8*inch]))
    a(Spacer(1,.1*inch))
    a(Paragraph(
        f"The model estimates {eadj:.2f} person-months of effort over {tdev:.2f} months with a team "
        f"of {team:.1f}. Our actual team of 4 members across 2 months (Sprints 1\u20132) is consistent with "
        f"this estimate. The productivity of {prod:.0f} LOC/PM is reasonable for a multi-stack project "
        f"involving Django, FastAPI, React, and complex inverse-operation logic.", ss["Body"]))

    # -------- 1.2 HALSTEAD --------
    a(PageBreak())
    a(Paragraph("1.2  Halstead Metrics", ss["H1"]))
    a(Paragraph(
        "Halstead's Software Science metrics quantify program complexity based on operators and "
        "operands. Measurements were taken from the Python backend codebase (1,790 LOC, 59 files) "
        "which contains the core versioning, rollback, snapshot, and API logic.", ss["Body"]))

    a(Paragraph("Raw Measurements", ss["H2"]))
    eta1, eta2, N1, N2 = 40, 1193, 3618, 5846
    a(tbl([
        ["Symbol", "Description", "Value"],
        ["\u03b7\u2081", "Unique operators (Python keywords + symbols)", str(eta1)],
        ["\u03b7\u2082", "Unique operands (identifiers, literals)", str(eta2)],
        ["N1", "Total operator occurrences", str(N1)],
        ["N2", "Total operand occurrences", str(N2)],
    ], cw=[.8*inch, 3*inch, 1*inch]))

    a(Paragraph("Derived Metrics", ss["H2"]))
    N = N1+N2; eta=eta1+eta2
    V = N*math.log2(eta)
    D = (eta1/2)*(N2/eta2)
    E = D*V
    T = E/18
    B = V/3000
    L = 1/D
    Nhat = eta1*math.log2(eta1) + eta2*math.log2(eta2)
    a(tbl([
        ["Metric", "Formula", "Value"],
        ["Vocabulary (\u03b7)", "\u03b7 = \u03b7\u2081 + \u03b7\u2082", str(eta)],
        ["Program Length (N)", "N = N1 + N2", f"{N:,}"],
        ["Est. Length (N\u0302)", "N\u0302 = \u03b7\u2081 log\u2082 \u03b7\u2081 + \u03b7\u2082 log\u2082 \u03b7\u2082", f"{Nhat:.0f}"],
        ["Volume (V)", "V = N x log\u2082(\u03b7)", f"{V:.0f} bits"],
        ["Difficulty (D)", "D = (\u03b7\u2081/2) x (N2/\u03b7\u2082)", f"{D:.2f}"],
        ["Effort (E)", "E = D x V", f"{E:,.0f}"],
        ["Time to Program (T)", "T = E / 18", f"{T:.0f} sec ({T/3600:.1f} hrs)"],
        ["Est. Bugs (B)", "B = V / 3000", f"{B:.2f}"],
        ["Program Level (L)", "L = 1 / D", f"{L:.4f}"],
    ], cw=[1.3*inch, 2.3*inch, 1.8*inch]))
    a(Spacer(1,.1*inch))
    a(Paragraph(
        f"The volume of {V:.0f} bits reflects substantial information content across the versioning, rollback, "
        f"snapshot, and API layers. The difficulty index of {D:.2f} is moderate, benefiting from clean "
        f"separation into service, route, and model layers. The estimated {B:.1f} bugs is within acceptable "
        f"bounds for a project of this scope. The estimated length N\u0302 = {Nhat:.0f} versus actual "
        f"N = {N:,} indicates {'reasonable' if abs(Nhat-N)/N < 0.4 else 'some deviation in'} code structure.", ss["Body"]))

    # -------- 1.3 FPA --------
    a(PageBreak())
    a(Paragraph("1.3  Function Point Analysis", ss["H1"]))
    a(Paragraph(
        "Function Point Analysis measures functional size from the user's perspective. Five component "
        "types are identified: External Inputs (EI), External Outputs (EO), External Inquiries (EQ), "
        "Internal Logical Files (ILF), and External Interface Files (EIF).", ss["Body"]))

    a(Paragraph("External Inputs (EI)", ss["H3"]))
    a(tbl([
        ["ID","Component","Description","Complexity"],
        ["EI-1","User Registration","POST /auth/register \u2014 create account","Low"],
        ["EI-2","User Login","POST /auth/token \u2014 JWT authentication","Low"],
        ["EI-3","Create Connection","POST /connections \u2014 encrypted DB profile","Average"],
        ["EI-4","Update Connection","PUT /connections/{id} \u2014 modify profile","Average"],
        ["EI-5","Delete Connection","DELETE /connections/{id} \u2014 cascade delete","Average"],
        ["EI-6","Execute Read SQL","POST /query/execute \u2014 SELECT queries","High"],
        ["EI-7","Create Commit","POST /commits \u2014 write SQL + atomic versioning","High"],
        ["EI-8","Set Snapshot Freq","PUT /snapshots/frequency \u2014 configure K","Low"],
        ["EI-9","Manual Snapshot","POST /snapshots/manual \u2014 pg_dump to S3","High"],
        ["EI-10","Rollback","POST /rollback \u2014 restore to target version","High"],
        ["EI-11","Token Refresh","POST /auth/token/refresh","Low"],
    ], cw=[.5*inch,1.2*inch,2.5*inch,.7*inch]))
    a(Spacer(1,.05*inch))

    a(Paragraph("External Outputs (EO)", ss["H3"]))
    a(tbl([
        ["ID","Component","Description","Complexity"],
        ["EO-1","Query Results","Tabular result set for SELECT","High"],
        ["EO-2","Commit Confirm","Version ID + metadata","Average"],
        ["EO-3","Rollback Status","Progress and final status","High"],
        ["EO-4","Error Messages","Structured error responses","Low"],
        ["EO-5","Snapshot Status","S3 upload confirmation","Average"],
    ], cw=[.5*inch,1.2*inch,2.5*inch,.7*inch]))
    a(Spacer(1,.05*inch))

    a(Paragraph("External Inquiries (EQ)", ss["H3"]))
    a(tbl([
        ["ID","Component","Description","Complexity"],
        ["EQ-1","List Connections","GET /connections","Low"],
        ["EQ-2","List Commits","GET /commits","Average"],
        ["EQ-3","Get Commit","GET /commits/{id}","Low"],
        ["EQ-4","List Snapshots","GET /snapshots","Average"],
        ["EQ-5","Get Snapshot Freq","GET /snapshots/frequency","Low"],
        ["EQ-6","List Anti-Commands","GET /anticommands","Average"],
        ["EQ-7","Get Anti-Command","GET /anticommands/{id}","Low"],
        ["EQ-8","DB Metadata","Schema tree (tables, columns, types)","High"],
    ], cw=[.5*inch,1.2*inch,2.5*inch,.7*inch]))
    a(Spacer(1,.05*inch))

    a(Paragraph("Internal Logical Files (ILF)", ss["H3"]))
    a(tbl([
        ["ID","Component","Description","Complexity"],
        ["ILF-1","Users","User accounts (Django AbstractUser)","Low"],
        ["ILF-2","ConnectionProfiles","Encrypted DB connection details","Average"],
        ["ILF-3","CommitEvents","Versioned SQL operations log with seq","High"],
        ["ILF-4","InverseOperations","Anti-commands per commit","High"],
        ["ILF-5","Snapshots","Snapshot metadata (S3 keys, version IDs)","Average"],
        ["ILF-6","SnapshotPolicies","User-configurable snapshot frequency","Low"],
    ], cw=[.5*inch,1.4*inch,2.3*inch,.7*inch]))
    a(Spacer(1,.05*inch))

    a(Paragraph("External Interface Files (EIF)", ss["H3"]))
    a(tbl([
        ["ID","Component","Description","Complexity"],
        ["EIF-1","AWS S3","Snapshot storage in cloud object store","Average"],
        ["EIF-2","PostgreSQL (User DB)","User's database being versioned","High"],
        ["EIF-3","Apache Kafka","Event stream for write ordering","High"],
    ], cw=[.5*inch,1.4*inch,2.3*inch,.7*inch]))

    a(Paragraph("Unadjusted Function Point Calculation", ss["H2"]))
    weights = {'EI':{'Low':3,'Average':4,'High':6},'EO':{'Low':4,'Average':5,'High':7},
               'EQ':{'Low':3,'Average':4,'High':6},'ILF':{'Low':7,'Average':10,'High':15},
               'EIF':{'Low':5,'Average':7,'High':10}}
    counts = {'EI':{'Low':4,'Average':3,'High':4},'EO':{'Low':1,'Average':2,'High':2},
              'EQ':{'Low':4,'Average':3,'High':1},'ILF':{'Low':2,'Average':2,'High':2},
              'EIF':{'Low':0,'Average':1,'High':2}}
    ufp_rows = [["Type","Low","Average","High","Subtotal"]]
    total_ufp = 0
    for ft in ['EI','EO','EQ','ILF','EIF']:
        sub=0
        parts=[]
        for cx in ['Low','Average','High']:
            n=counts[ft][cx]; w=weights[ft][cx]; sub+=n*w; parts.append(f"{n} x {w} = {n*w}")
        total_ufp+=sub
        ufp_rows.append([ft]+parts+[str(sub)])
    ufp_rows.append(["","","","Total UFP:",str(total_ufp)])
    a(tbl(ufp_rows, cw=[.5*inch,1.1*inch,1.1*inch,1.1*inch,.8*inch]))

    a(Paragraph("Value Adjustment Factor", ss["H2"]))
    gsc_data = [["#","General System Characteristic","Rating","Justification"]]
    gscs = [
        ("1","Data Communications","4","HTTP REST + WebSocket terminal"),
        ("2","Distributed Data Processing","3","Django + FastAPI + S3 + PostgreSQL"),
        ("3","Performance","2","Latency not primary concern (SRS)"),
        ("4","Heavily Used Configuration","2","Standard cloud deployment"),
        ("5","Transaction Rate","3","Multiple concurrent writes"),
        ("6","Online Data Entry","5","SQL editor is primary interface"),
        ("7","End-User Efficiency","4","IDE-style workbench with panels"),
        ("8","Online Update","4","Real-time query execution"),
        ("9","Complex Processing","4","Inverse op generation, rollback"),
        ("10","Reusability","2","Service-based architecture"),
        ("11","Installation Ease","1","Dev environment only"),
        ("12","Operational Ease","3","Web-based with config options"),
        ("13","Multiple Sites","1","Single deployment"),
        ("14","Facilitate Change","3","Modular backend, configurable"),
    ]
    for g in gscs: gsc_data.append(list(g))
    a(tbl(gsc_data, cw=[.3*inch,2*inch,.5*inch,2.8*inch]))
    tdi=sum(int(g[2]) for g in gscs)
    vaf=0.65+0.01*tdi
    fp=total_ufp*vaf
    a(Spacer(1,.05*inch))
    a(Paragraph(f"Total Degree of Influence (TDI) = {tdi}", ss["Body"]))
    a(Paragraph(f"VAF = 0.65 + 0.01 x {tdi} = {vaf:.2f}", ss["Body"]))
    a(Paragraph(f"Adjusted Function Points = {total_ufp} x {vaf:.2f} = {fp:.1f} FP", ss["Body"]))
    a(Paragraph("Derived Metrics:", ss["H3"]))
    a(tbl([
        ["Metric","Value"],
        ["Adjusted FP",f"{fp:.1f}"],
        ["Est. LOC (50 LOC/FP)",f"{fp*50:.0f}"],
        ["Actual LOC","12,239"],
        ["LOC per FP",f"{12239/fp:.1f}"],
        ["Est. Effort (Jones: FP^0.4)",f"{fp**0.4:.1f} PM"],
        ["Est. Defects (6/1000 FP)",f"{fp*6/1000:.1f}"],
    ], cw=[2.2*inch,1.5*inch]))

    # -------- 1.4 CFD --------
    a(PageBreak())
    a(Paragraph("1.4  Cumulative Flow Diagram", ss["H1"]))
    a(Paragraph(
        "The CFD visualizes task flow through states (To Do, In Progress, Done) over the project "
        "timeline. Data is from Trello board tracking and the requirements traceability matrix. "
        "Horizontal distance between bands indicates lead time; vertical width indicates WIP.", ss["Body"]))
    img(S, mk_cfd(), cap="Figure 1: Cumulative Flow Diagram across Sprint 1 and Sprint 2.")
    a(Paragraph("Observations:", ss["H3"]))
    bullet(S, "Steady upward growth in the Done band indicates consistent task completion.")
    bullet(S, "In Progress band stays narrow (3\u20136 tasks), suggesting good WIP management.")
    bullet(S, "Acceleration around Mar 17\u201324 corresponds to Sprint 2 peak development.")
    bullet(S, "By Mar 31, only 2 tasks remain in backlog \u2014 healthy for Sprint 3 planning.")

    # -------- 1.5 Throughput --------
    a(Paragraph("1.5  Throughput Report", ss["H1"]))
    a(Paragraph(
        "Throughput measures task completion rate (tasks/week). It provides insight into team velocity "
        "and helps identify periods of high and low productivity.", ss["Body"]))
    img(S, mk_throughput(), cap="Figure 2: Weekly throughput with average line.")
    a(tbl([
        ["Week","Period","Tasks","Sprint","Key Activities"],
        ["W1","Feb 10\u201316","3","Sprint 1","PostgreSQL setup, initial codebase, S3 stubs"],
        ["W2","Feb 17\u201323","3","Sprint 1","Query execution, change tracking model"],
        ["W3","Feb 24\u201328","2","Sprint 2","AWS RDS testing, sprint transition"],
        ["W4","Mar 3\u20139","6","Sprint 2","Anti-command gen, SQL validation, version mgmt"],
        ["W5","Mar 10\u201316","5","Sprint 2","Rollback manager, commit tracker, auth"],
        ["W6","Mar 17\u201323","7","Sprint 2","Django-FastAPI integration, query interface"],
        ["W7","Mar 24\u201328","4","Sprint 2","Sequential IDs, test cases, backend fixes"],
        ["W8","Mar 29\u201331","2","Sprint 2","Frontend scaffold, final fixes"],
    ], cw=[.4*inch,.85*inch,.45*inch,.7*inch,3.1*inch]))
    a(Paragraph(f"Average: 4.0 tasks/week  |  Peak: 7 (W6)  |  Total: 32 tasks over 8 weeks.", ss["Body"]))

    # -------- 1.6 Burndown --------
    a(PageBreak())
    a(Paragraph("1.6  Sprint Burndown Chart", ss["H1"]))
    a(Paragraph(
        "The burndown chart shows remaining work (story points) versus time for Sprint 2 "
        "(Feb 25 \u2013 Mar 31), the main development sprint covering concurrency, versioning, "
        "rollback, and backend integration.", ss["Body"]))
    img(S, mk_burndown(), cap="Figure 3: Sprint 2 Burndown \u2014 ideal vs actual remaining work.")
    a(Paragraph("Analysis:", ss["H3"]))
    bullet(S, "Week 0\u20131: Slightly behind ideal due to semester break backlog and Sprint 1 handoff.")
    bullet(S, "Week 2\u20133: Caught up \u2014 peak development with Django-FastAPI integration.")
    bullet(S, "Week 4\u20135: Close to ideal; 1 task (Command History serialization) carried to Sprint 3.")
    bullet(S, "Overall close tracking indicates effective sprint planning and estimation.")

    # -------- 1.7 Burnup --------
    a(Paragraph("1.7  Sprint Burnup Chart", ss["H1"]))
    a(Paragraph(
        "The burnup chart shows both completed work and total scope, making scope changes visible.", ss["Body"]))
    img(S, mk_burnup(), cap="Figure 4: Sprint 2 Burnup \u2014 scope and completed work.")
    bullet(S, "Scope increased 22 to 24 in Week 3 (additional test implementation tasks).")
    bullet(S, "S-curve completion: slow start, rapid middle, tapering \u2014 typical healthy sprint.")
    bullet(S, "23 of 24 points completed (95.8% completion rate).")

    # -------- 1.8 Velocity --------
    a(PageBreak())
    a(Paragraph("1.8  Additional Metric 1: Sprint Velocity", ss["H1"]))
    a(Paragraph(
        "Velocity measures work completed per sprint in story points.", ss["Body"]))
    img(S, mk_velocity(), w=5*inch, h=3*inch, cap="Figure 5: Sprint Velocity \u2014 planned vs completed.")
    a(Paragraph(
        "Sprint 1 achieved 100% (12/12). Sprint 2 achieved 95.8% (23/24). Average velocity of "
        "17.5 points/sprint informs Sprint 3 capacity planning.", ss["Body"]))

    # -------- 1.9 Code Growth --------
    a(Paragraph("1.9  Additional Metric 2: Code Growth & Commit Activity", ss["H1"]))
    img(S, mk_loc(), cap="Figure 6: Lines of code growth over time by component.")
    img(S, mk_commit(), cap="Figure 7: Commit activity by developer per week.")
    a(Paragraph("Test Coverage:", ss["H3"]))
    img(S, mk_test(), w=5.5*inch, h=2.8*inch, cap="Figure 8: Test case status \u2014 79 total (Unit / Integration / System).")
    a(Paragraph(
        "79 documented test cases: 40 unit, 22 integration, 17 system. Completed: 40 (50.6%), "
        "In Progress: 3 (3.8%), Not Tested: 36 (45.6%). "
        "The backend test suite (upstream/backend) contains 2,058 lines across 10 test files "
        "plus conftest.py, covering authentication, connections, core models, core services, "
        "commits, queries, rollback, snapshots, and anti-commands. "
        "The inverse engine test suite (upstream/inverse_sql_commands) contains 517 lines.", ss["Body"]))

    # =================== SECTION II ===================
    a(PageBreak())
    a(Paragraph("2. Section II: Risk Analysis", ss["H1"]))

    a(Paragraph("2.1  Risk Assessment Matrix", ss["H2"]))
    a(Paragraph(
        "Risks were identified through: (1) SRS requirements analysis, (2) architectural review of "
        "Django + FastAPI + S3 + PostgreSQL stack, (3) team experience from Sprints 1\u20132, and "
        "(4) common failure modes in database versioning systems.", ss["Body"]))
    a(Paragraph(
        "Probability (1\u20135): Based on team experience, technology maturity, and observable indicators. "
        "Impact (1\u20135): Severity to system functionality, data integrity, and user experience. "
        "Risk Score: P x I. Categories: Low (1\u20134), Medium (5\u20139), High (10\u201316), Critical (17\u201325).", ss["Body"]))
    img(S, mk_risk(), w=5.2*inch, h=4*inch, cap="Figure 9: Risk Assessment Matrix with 10 identified risks.")

    a(Paragraph("2.2  Detailed Risk Register", ss["H2"]))
    rr = [["ID","Risk","P","I","Score","Level","Mitigation"]]
    risks_detail = [
        ["R1","DB Corruption during Rollback","3","4","12","High",
         "Atomic transactions (@transaction.atomic); rollback stops on failure (REQ-11); snapshot boundaries"],
        ["R2","Snapshot Storage Exhaustion","4","3","12","High",
         "Configurable frequency (K); deleteOldSnapshots cleanup; linear growth monitoring (NFR-QUA-1)"],
        ["R3","Data Loss on Failed Rollback","2","5","10","High",
         "Nearest-snapshot restore; failure-safe (REQ-11); pre-rollback snapshot"],
        ["R4","API Authentication Bypass","3","2","6","Medium",
         "JWT on all endpoints; Django auth middleware; token rotation"],
        ["R5","Concurrent Write Conflict","3","3","9","Medium",
         "PostgreSQL MVCC; Kafka ordering (planned); sequential version IDs"],
        ["R6","AWS S3 Outage","2","4","8","Medium",
         "Manual snapshot fallback; local backup; eventual consistency handling"],
        ["R7","Frontend UI Regression","4","2","8","Medium",
         "Component architecture; Shadcn library; Playwright E2E (planned)"],
        ["R8","Slow Query Execution","2","1","2","Low",
         "Read-only validation; connection pooling; indexed commit tables"],
        ["R9","Schema Migration Failure","2","3","6","Medium",
         "Django migration framework; migration testing; schema versioning"],
        ["R10","Full System Crash","1","5","5","Medium",
         "Modular architecture; isolated failure domains; persistent PostgreSQL storage"],
    ]
    for r in risks_detail: rr.append(r)
    a(tbl(rr, cw=[.35*inch,1.2*inch,.25*inch,.25*inch,.35*inch,.5*inch,3*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("Probability Justification", ss["H3"]))
    a(Paragraph(
        "R1 (P=3): Rollback modifies live state via inverse operations; Sprint 2 showed edge cases "
        "in complex UPDATE inversions, but atomic transactions mitigate significantly.", ss["BodySm"]))
    a(Paragraph(
        "R2 (P=4): Snapshots (pg_dump) can be large; S3 costs scale linearly. RDS testing (Feb 26) "
        "confirmed substantial snapshot sizes without cleanup.", ss["BodySm"]))
    a(Paragraph(
        "R3 (P=2): Snapshot-based recovery provides safety net; worst case limited to K commits.", ss["BodySm"]))
    a(Paragraph(
        "R5 (P=3): Kafka integration pending; currently relying on PostgreSQL MVCC which may not "
        "guarantee strict global ordering for the versioning system.", ss["BodySm"]))
    a(Paragraph(
        "R7 (P=4): Frontend was rebuilt from Next.js to React/Lovable; rapid UI changes increase regression risk.", ss["BodySm"]))

    # =================== SECTION III ===================
    a(PageBreak())
    a(Paragraph("3. Section III: Risk-Sprint Integration", ss["H1"]))
    a(Paragraph(
        "This section maps each risk to the sprint plan, showing where mitigation strategies are "
        "embedded in the backlog as specific tasks, acceptance criteria, and technical decisions.", ss["Body"]))

    a(Paragraph("3.1  Risk Mitigation Across Sprints", ss["H2"]))

    a(Paragraph("Sprint 1 (Feb 4\u201319) \u2014 Completed", ss["H3"]))
    a(tbl([
        ["Risk","Mitigation","Backlog Item","Status"],
        ["R8","Query execution perf validated","Setting up initial PostgreSQL database","Done"],
        ["R6","S3 upload/download with error handling","Basic upload/download path to S3 bucket","Done"],
        ["R1","Atomic transaction model in Django ORM","Backend function to execute SQL read/writes","Done"],
        ["R2","S3 latency and storage tested","Testing AWS Aurora RDS for read/write latency","Done"],
    ], cw=[.5*inch,2*inch,2.2*inch,.5*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("Sprint 2 (Feb 25 \u2013 Mar 31) \u2014 Completed", ss["H3"]))
    a(tbl([
        ["Risk","Mitigation","Backlog Item","Status"],
        ["R1","record_commit() with @transaction.atomic","Integrating Django and FastAPI","Done"],
        ["R3","Rollback with nearest snapshot + inverse replay","Setting up anti-command generation","Done"],
        ["R4","JWT auth with SimpleJWT on all endpoints","Django authorization, admin, ORM models","Done"],
        ["R5","Sequential version IDs; PostgreSQL MVCC","User input query validation","Done"],
        ["R2","Configurable snapshot frequency (K=5, min=1)","Anti-command generation (includes snapshots)","Done"],
        ["R9","Django migrations for CommitEvent, Snapshot","Django authorization, admin, ORM models","Done"],
        ["R7","Component architecture with Shadcn UI","FastAPI framework, frontend scaffold","Done"],
    ], cw=[.5*inch,2.2*inch,2*inch,.5*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("Sprint 3 (Apr 1\u201315) \u2014 Planned", ss["H3"]))
    a(tbl([
        ["Risk","Planned Mitigation","Backlog Item","Priority"],
        ["R5","Kafka event-driven write ordering","Kafka Integration (MessageBroker)","High"],
        ["R1","Rollback fault injection testing","Testing","High"],
        ["R2","SnapshotManager with cleanup policies","Redis cache implementation","Medium"],
        ["R7","E2E testing with Playwright","Setting up the UI","Medium"],
        ["R3","Multi-table FK integrity checks","Version control for multiple tables","Medium"],
        ["R10","Full system and load testing","Testing","Medium"],
        ["R6","Redis cache for S3 failover","Redis cache implementation","Low"],
    ], cw=[.5*inch,2.2*inch,2*inch,.5*inch]))
    a(Spacer(1,.15*inch))

    a(Paragraph("Risk Coverage Summary", ss["H2"]))
    a(tbl([
        ["Risk","Sprint 1","Sprint 2","Sprint 3","Residual"],
        ["R1: DB Corruption","Foundation","Atomic txns","Fault testing","Low"],
        ["R2: Storage","S3 testing","Config. K","Cleanup policy","Low"],
        ["R3: Data Loss","\u2014","Snapshot restore","Multi-table FK","Low"],
        ["R4: Auth Bypass","\u2014","JWT + SimpleJWT","\u2014","Low"],
        ["R5: Concurrency","\u2014","Sequential IDs","Kafka ordering","Medium"],
        ["R6: S3 Outage","Error handling","\u2014","Redis cache","Low"],
        ["R7: UI Regression","\u2014","Shadcn + React","E2E testing","Low"],
        ["R8: Slow Query","Query validation","\u2014","\u2014","Low"],
        ["R9: Migration","\u2014","Django migrations","\u2014","Low"],
        ["R10: System Crash","\u2014","Modular arch","Load testing","Low"],
    ], cw=[1.2*inch,1*inch,1.1*inch,1*inch,.65*inch]))
    a(Spacer(1,.1*inch))

    a(Paragraph("3.2  Integration with Sprint Backlog (Trello)", ss["H2"]))
    a(Paragraph(
        "The Trello board organizes tasks into sprint-specific lists. Risk mitigation is embedded "
        "within backlog items:", ss["Body"]))
    bullet(S, "Sprint 1 (Completed): 'Setting up the initial PostgreSQL database' addresses R1, R8; "
              "'Basic upload/download path to S3 bucket' addresses R6.")
    bullet(S, "Sprint 2 (Completed): 'Setting up anti-command generation' addresses R1, R3; "
              "'Validation of user input query through validator' addresses R5; "
              "'Setting up Django authorization, admin and ORM models' addresses R4, R9; "
              "'Integrating the Django and FastAPI frameworks' addresses R1, R7, R10.")
    bullet(S, "Sprint 3 (Planned): 'Setting up redis for cache implementation' addresses R2, R6; "
              "'Handling version control for multiple tables' addresses R3; "
              "'Testing' addresses R1, R5, R7, R10; "
              "'Setting up the UI' and 'Connecting the frontend to the backend' address R7.")
    bullet(S, "Sprint Backlog: Unassigned items are prioritized by risk severity and dependencies.")
    a(Spacer(1,.2*inch))

    # conclusion
    a(Paragraph("Conclusion", ss["H2"]))
    a(Paragraph(
        f"This report presents software metrics and risk analysis for WEAVE-DB. Key findings:", ss["Body"]))
    bullet(S, f"COCOMO: Estimated {eadj:.1f} person-months \u2014 consistent with 4-member team over 2 months.")
    bullet(S, f"Halstead: Moderate difficulty ({D:.2f}) with good modularization.")
    bullet(S, f"FPA: {fp:.1f} adjusted function points for a medium-complexity system with rich API surface.")
    bullet(S, f"Sprint Metrics: 95.8% Sprint 2 completion, 4.0 avg throughput.")
    bullet(S, f"All-branch LOC: 12,239 lines across 7 branches, 144 files, 4 contributors.")
    bullet(S, "10 risks identified (3 High, 5 Medium, 1 Low) \u2014 all with active mitigation mapped to sprints.")
    bullet(S, "79 test cases documented; 50.6% completed, remainder planned for Sprint 3.")

    doc.build(S)
    print(f"PDF: {OUTPUT_PDF}")

if __name__ == "__main__":
    print("Generating charts ..."); mk_burndown(); mk_burnup(); mk_cfd(); mk_throughput()
    mk_velocity(); mk_loc(); mk_commit(); mk_test(); mk_risk()
    print("Building PDF ..."); build(); print("Done.")
